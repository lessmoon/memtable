#ifndef MEM_TABLE_H
#define MEM_TABLE_H

#include <deque>
#include <tuple>
#include <initializer_list>
#include <type_traits>
#include <memory>
#include <functional>
#include <iostream>
#include <typeinfo>
#include <algorithm>

#include <cxxabi.h>

#include "utils.hpp"

template <typename Field>
struct CheckField
{
    static_assert(__builtin_strcmp(Field::name, "") != 0, "Field name is not complete");
    static_assert(!std::is_same<typename Field::type, void>::value, "Field type is error");
};

#define DEF_Field(n, t, id)             \
    struct id                           \
    {                                   \
        constexpr static auto name = n; \
        using type = t;                 \
    };

//Some fields will be packed by tuple
template <typename UpStream, typename SortFields, typename... Fields>
class SortStream;
//func(UpStream)->Fields...
template <typename UpStream, typename... Fields>
class MapStream;

template <typename UpStream, typename FilterFields, typename... Fields>
class FilterStream;

template <typename UpStream, typename... Fields>
class SkipStream;
//TODO: remove Fields
template <typename UpStream, typename... Fields>
class LimitStream;

void debug(IndexType<>)
{
}

template <size_t I, size_t... N>
void debug(IndexType<I, N...>)
{
    std::cout << I << " ";
    debug(IndexType<N...>{});
}

template <typename Tuple, size_t I = 0>
void debug(Tuple t)
{
    std::cout << std::get<I>(t) << " |";
    if constexpr (I + 1 < std::tuple_size<Tuple>::value)
        debug<Tuple, I + 1>(t);
}

template <typename... Fields>
class MemoryTable;

template <typename Derived, typename Iterator, typename... Fields>
class Stream
{
public:
    using field_types = std::tuple<Fields...>;
    using value_type = std::tuple<typename Fields::type...>;

public:
    void for_each(std::function<void(typename Fields::type...)> func)
    {
        Iterator iter = iterator();
        while (iter)
        {
            apply_all(func, iter.value());
            iter.next();
        }
    }
    template <typename Result>
    Result reduce(std::function<Result(const Result &, const typename Fields::type &...)> func, Result &&init)
    {
        Iterator iter = iterator();
        Result res = init;
        while (iter)
        {
            res = apply_all_ex(func, res, iter.value());
            iter.next();
        }
        return res;
    }

    template <typename Result, typename... ReduceFields>
    Result reduce_by(std::function<Result(const Result &, const typename ReduceFields::type &...)> func, Result &&init)
    {
        return select<ReduceFields...>().reduce(func, std::forward<Result>(init));
    }
    size_t count()
    {
        size_t c = 0;
        for (Iterator iter = iterator(); iter; iter.next())
        {
            c++;
        }
        return c;
    }
    MemoryTable<Fields...> collect()
    {
        MemoryTable<Fields...> table;
        Iterator iter = iterator();
        while (iter)
        {
            table.push(iter.value());
            iter.next();
        }
        return table;
    }

    SkipStream<Derived, Fields...> skip(size_t step)
    {
        return SkipStream<Derived, Fields...>(*static_cast<Derived *>(this), step);
    }
    LimitStream<Derived, Fields...> limit(size_t size)
    {
        return LimitStream<Derived, Fields...>(*static_cast<Derived *>(this), size);
    }
    template <typename... SelectFields>
    MapStream<Derived, SelectFields...> select()
    {
        return map<SelectFields...>([](const typename Fields::type &... vals) -> std::tuple<typename SelectFields::type...> {
            return {std::get<TypeIndexInTuple<std::tuple<Fields...>, SelectFields>::value>(std::make_tuple(vals...))...};
        });
    }

    SortStream<Derived, std::tuple<Fields...>, Fields...> sort(std::function<bool(const typename Fields::type &..., const typename Fields::type &...)> more)
    {
        return sort_by<Fields...>(more);
    }

    template <typename... CompareFields>
    SortStream<Derived, std::tuple<CompareFields...>, Fields...> sort_by(std::function<bool(const typename CompareFields::type &..., const typename CompareFields::type &...)> more)
    {
        return SortStream<Derived, std::tuple<CompareFields...>, Fields...>(*static_cast<Derived *>(this), more);
    }

    template <typename... OrderFields>
    SortStream<Derived, std::tuple<OrderFields...>, Fields...> order_by()
    {
        return sort_by<OrderFields...>([](const typename OrderFields::type &... args1, const typename OrderFields::type &... args2) -> bool {
            //FIXME:
            return ((args1 > args2) && ...);
        });
    }

    FilterStream<Derived, Fields...> filter(std::function<bool(const typename Fields::type &...)> func)
    {
        return filter_by<Derived, std::tuple<Fields...>, Fields...>(func);
    }

    template <typename... FilterFields>
    FilterStream<Derived, std::tuple<FilterFields...>, Fields...> filter_by(std::function<bool(const typename FilterFields::type &...)> func)
    {
        return FilterStream<Derived, std::tuple<FilterFields...>, Fields...>(*static_cast<Derived *>(this), func);
    }

    template <typename... MapFields>
    MapStream<Derived, MapFields...> map(std::function<std::tuple<typename MapFields::type...>(const typename Fields::type &...)> func)
    {
        return MapStream<Derived, MapFields...>(*static_cast<Derived *>(this), func);
    }

    Iterator iterator()
    {
        return static_cast<Derived *>(this)->iterator_impl();
    }

    std::string debug()
    {
        std::string out = abi::__cxa_demangle(typeid(Derived).name(), NULL, NULL, NULL);
        return out;
    }
};
template <typename... Fields>
struct MemoryTableIterator
{
    using value_type = std::tuple<typename Fields::type...>;
    using iter_type = typename std::deque<value_type>::const_iterator;

    iter_type _begin;
    iter_type _end;
    MemoryTableIterator() : _begin(), _end()
    {
    }
    MemoryTableIterator(const MemoryTableIterator<Fields...> &other) = default;
    MemoryTableIterator(MemoryTableIterator<Fields...> &&other) = default;

    MemoryTableIterator(iter_type &&begin, iter_type &&end)
        : _begin(begin), _end(end)
    {
    }

    MemoryTableIterator(const iter_type &begin, const iter_type &end)
        : _begin(begin), _end(end)
    {
    }

    MemoryTableIterator<Fields...> &operator=(MemoryTableIterator<Fields...> &&other) = default;
    MemoryTableIterator<Fields...> &operator=(const MemoryTableIterator<Fields...> &other) = default;

    explicit operator bool() const noexcept
    {
        return _begin != _end;
    }

    void next()
    {
        ++_begin;
    }

    const value_type &value() const
    {
        return *_begin;
    }
};

template <typename... Fields>
class MemoryTableStream : public Stream<MemoryTableStream<Fields...>, MemoryTableIterator<Fields...>, Fields...>
{
    const MemoryTable<Fields...> *_ptr;

public:
    using Iterator = MemoryTableIterator<Fields...>;

public:
    Iterator iterator_impl()
    {
        return Iterator(_ptr->_details.cbegin(), _ptr->_details.cend());
    }

public:
    MemoryTableStream(const MemoryTable<Fields...> *mem_table) : _ptr(mem_table)
    {
    }
    MemoryTableStream(const MemoryTableStream<Fields...> &other) = default;

    MemoryTableStream(MemoryTableStream<Fields...> &&other) = default;
};

template <typename UpStream, typename... Fields>
class SkipStream : public Stream<SkipStream<UpStream, Fields...>, typename UpStream::Iterator, Fields...>
{
    UpStream _up;
    size_t _step;

public:
    using Iterator = typename UpStream::Iterator;

public:
    Iterator iterator_impl()
    {
        auto iter = _up.iterator();
        while (iter && _step-- > 0)
        {
            iter.next();
        }
        return iter;
    }

    SkipStream<UpStream, Fields...> skip(size_t step)
    {
        auto tmp{std::cref(*this).get()};
        tmp._step += step;
        return tmp;
    }

public:
    SkipStream(UpStream &&up, size_t step) : _up(up), _step(step)
    {
    }
    SkipStream(const UpStream &up, size_t step) : _up(up), _step(step)
    {
    }
    SkipStream(const SkipStream<UpStream, Fields...> &) = default;
    SkipStream(SkipStream<UpStream, Fields...> &&) = default;
    SkipStream<UpStream, Fields...> &operator=(const SkipStream<UpStream, Fields...> &) = default;
    SkipStream<UpStream, Fields...> &operator=(SkipStream<UpStream, Fields...> &&) = default;
};

template <typename UpperIterator>
struct LimitIterator
{
private:
    UpperIterator _iter;
    size_t _left;

public:
    using value_type = typename UpperIterator::value_type;
    LimitIterator(LimitIterator<UpperIterator> &&other) = default;
    LimitIterator(const LimitIterator<UpperIterator> &other) = default;
    LimitIterator(UpperIterator &&iter, size_t left) : _iter(std::forward<UpperIterator>(iter)), _left(left)
    {
    }
    LimitIterator(const UpperIterator &iter, size_t left) : _iter(iter), _left(left)
    {
    }
    LimitIterator<UpperIterator> &operator=(LimitIterator<UpperIterator> &&other) = default;
    LimitIterator<UpperIterator> &operator=(const LimitIterator<UpperIterator> &other) = default;
    explicit operator bool() const noexcept
    {
        return _left > 0 && _iter.operator bool();
    }

    void next()
    {
        _iter.next();
        _left--;
    }

    auto value() -> decltype(_iter.value())
    {
        return _iter.value();
    }
    auto value() const -> decltype(_iter.value())
    {
        return _iter.value();
    }
};

template <typename UpStream, typename... Fields>
class LimitStream : public Stream<LimitStream<UpStream, Fields...>, LimitIterator<typename UpStream::Iterator>, Fields...>
{
    UpStream _up;
    size_t _limit;

public:
    using Iterator = LimitIterator<typename UpStream::Iterator>;

public:
    Iterator iterator_impl()
    {
        auto iter = Iterator(_up.iterator(), _limit);
        return iter;
    }

    LimitStream<UpStream, Fields...> limit(size_t size)
    {
        if (size >= _limit)
        {
            return *this;
        }
        else
        {
            auto tmp{std::cref(*this).get()};
            tmp._limit = size;
            return tmp;
        }
    }

public:
    LimitStream(UpStream &&up, size_t limit) : _up(up), _limit(limit)
    {
    }
    LimitStream(const UpStream &up, size_t limit) : _up(up), _limit(limit)
    {
    }

    LimitStream(const LimitStream<UpStream, Fields...> &other) = default;
    LimitStream(LimitStream<UpStream, Fields...> &&other) = default;
    LimitStream<UpStream, Fields...> &operator=(const LimitStream<UpStream, Fields...> &other) = default;
    LimitStream<UpStream, Fields...> &operator=(LimitStream<UpStream, Fields...> &&other) = default;
};

template <typename UpperIterator, typename Tuple, typename... Fields>
struct MapIterator
{
public:
    using func_type = std::function<Tuple(const typename Fields::type &...)>;

private:
    UpperIterator _iter;
    func_type _func;

public:
    using value_type = Tuple;
    MapIterator(MapIterator<UpperIterator, Tuple, Fields...> &&other) = default;
    MapIterator(const MapIterator<UpperIterator, Tuple, Fields...> &other) = default;
    MapIterator(UpperIterator &&iter, func_type func) : _iter(std::forward<UpperIterator>(iter)), _func(func)
    {
    }
    MapIterator(const UpperIterator &iter, func_type func) : _iter(iter), _func(func)
    {
    }
    MapIterator<UpperIterator, Tuple, Fields...> &operator=(MapIterator<UpperIterator, Tuple, Fields...> &&other) = default;
    MapIterator<UpperIterator, Tuple, Fields...> &operator=(const MapIterator<UpperIterator, Tuple, Fields...> &other) = default;
    explicit operator bool() const noexcept
    {
        return _iter.operator bool();
    }

    void next()
    {
        _iter.next();
    }

    Tuple value()
    {
        return apply_all(_func, _iter.value());
    }
    Tuple value() const
    {
        return apply_all(_func, _iter.value());
    }
};

template <typename UpStream, typename TupleResult, typename Index>
struct MapIteratorTraits;

template <typename UpStream, typename TupleResult, size_t... I>
struct MapIteratorTraits<UpStream, TupleResult, IndexType<I...>>
{
    using type = MapIterator<typename UpStream::Iterator, TupleResult, typename std::tuple_element<I, typename UpStream::field_types>::type...>;
};

template <typename UpStream, typename... Fields>
using GenMapIterator = typename MapIteratorTraits<UpStream, std::tuple<typename Fields::type...>,
                                                  typename IndexGen<std::tuple_size<typename UpStream::field_types>::value>::type>::type;

template <typename UpStream, typename... Fields>
class MapStream : public Stream<MapStream<UpStream, Fields...>, GenMapIterator<UpStream, Fields...>, Fields...>
{
public:
    using Iterator = GenMapIterator<UpStream, Fields...>;
    using func_type = typename Iterator::func_type;

private:
    UpStream _up;
    func_type _func;

public:
    Iterator iterator_impl()
    {
        auto iter = Iterator(_up.iterator(), _func);
        return iter;
    }

public:
    MapStream(UpStream &&up, func_type func) : _up(up), _func(func)
    {
    }
    MapStream(const UpStream &up, func_type func) : _up(up), _func(func)
    {
    }

    MapStream(const MapStream<UpStream, Fields...> &other) = default;
    MapStream(MapStream<UpStream, Fields...> &&other) = default;
    MapStream<UpStream, Fields...> &operator=(const MapStream<UpStream, Fields...> &other) = default;
    MapStream<UpStream, Fields...> &operator=(MapStream<UpStream, Fields...> &&other) = default;
};
template <typename Fields, typename Result>
struct FunctionTraits;
template <typename Result, typename... Fields>
struct FunctionTraits<std::tuple<Fields...>, Result>
{
    using const_func = std::function<Result(const typename Fields::type &...)>;
    using func = std::function<Result(typename Fields::type &...)>;
};

template <typename State, typename... Fields>
struct GeneratorIterator
{
    using value_type = std::tuple<typename Fields::type...>;
    using ret_type = std::pair<bool, value_type>;
    using func_type = std::function<ret_type(State&)>;
    func_type _func;
    State _internal_state;
    ret_type _cache;

    GeneratorIterator(func_type func, const State& init) : _func(func), _internal_state(init), _cache(_func(_internal_state)) {
    }
    GeneratorIterator(const GeneratorIterator<State, Fields...> &other) = default;
    GeneratorIterator(GeneratorIterator<State, Fields...> &&other) = default;

    GeneratorIterator<State, Fields...> &operator=(GeneratorIterator<State, Fields...> &&other) = default;
    GeneratorIterator<State, Fields...> &operator=(const GeneratorIterator<State, Fields...> &other) = default;

    explicit operator bool() const noexcept
    {
        return _cache.first;
    }

    void next()
    {
        _cache = _func(_internal_state);
    }

    const value_type &value() const
    {
        return _cache.second;
    }

    const value_type &value()
    {
        return _cache.second;
    }
};

template <typename State, typename... Fields>
class Generator : public Stream<Generator<State, Fields...>, GeneratorIterator<State, Fields...>, Fields...>
{
public:
    using Iterator = GeneratorIterator<State, Fields...>;
    using func_type = typename Iterator::func_type;
    using init_func_type = std::function<State()>;
private:
    typename Iterator::func_type _func;
    init_func_type _init_func;
public:
    Iterator iterator_impl()
    {
        return Iterator(_func, _init_func());
    }
public:
    Generator(func_type func, init_func_type init_func) : _func(func), _init_func(init_func)
    {
    }

    Generator(const Generator<State, Fields...> &other) = default;
    Generator(Generator<State, Fields...> &&other) = default;
    Generator<State, Fields...> &operator=(const Generator<State, Fields...> &other) = default;
    Generator<State, Fields...> &operator=(Generator<State, Fields...> &&other) = default;
};

template<typename State, typename... Fields>
Generator<State, Fields...> make_generator(const typename Generator<State, Fields...>::func_type func,
                                           const typename Generator<State, Fields...>::init_func_type init_func=[]() -> State {return State{};}) {
    return Generator<State, Fields...>(func, init_func);
}

template <typename UpStream, typename SortFields, typename... Fields>
class SortStream : public Stream<SortStream<UpStream, SortFields, Fields...>, typename MemoryTableStream<Fields...>::Iterator, Fields...>
{
public:
    using Iterator = typename MemoryTableStream<Fields...>::Iterator;
    using func_type = typename FunctionTraits<decltype(std::tuple_cat(SortFields(), SortFields())), bool>::const_func;

private:
    UpStream _up;
    func_type _func;

public:
    Iterator iterator_impl()
    {
        auto result = _up.collect();
        auto func_wrapper = [this](const std::tuple<typename Fields::type...> &a, const std::tuple<typename Fields::type...> &b) -> bool {
            using IndexT = typename TupleTypeIndicesInTypes<SortFields, Fields...>::type;
            return apply(_func, a, b, IndexT{});
        };
        std::sort(result._details.begin(), result._details.end(), func_wrapper);
        return result.stream().iterator();
    }

public:
    SortStream(UpStream &&up, func_type func) : _up(up), _func(func)
    {
    }
    SortStream(const UpStream &up, func_type func) : _up(up), _func(func)
    {
    }

    SortStream(const SortStream<UpStream, SortFields, Fields...> &other) = default;
    SortStream(SortStream<UpStream, SortFields, Fields...> &&other) = default;
    SortStream<UpStream, SortFields, Fields...> &operator=(const SortStream<UpStream, SortFields, Fields...> &other) = default;
    SortStream<UpStream, SortFields, Fields...> &operator=(SortStream<UpStream, SortFields, Fields...> &&other) = default;
};

template <typename UpperIterator, typename FilterFields, typename... Fields>
struct FilterIterator
{
    using func = typename FunctionTraits<FilterFields, bool>::const_func;
    using value_type = typename UpperIterator::value_type;

private:
    mutable UpperIterator _iter;
    func _func;
    using IndexType = typename TupleTypeIndicesInTypes<FilterFields, Fields...>::type;

public:
    FilterIterator(FilterIterator<UpperIterator, FilterFields, Fields...> &&other) = default;
    FilterIterator(const FilterIterator<UpperIterator, FilterFields, Fields...> &other) = default;
    FilterIterator(UpperIterator &&iter, func func) : _iter(std::forward<UpperIterator>(iter)), _func(func)
    {
    }
    FilterIterator(const UpperIterator &iter, func func) : _iter(iter), _func(func)
    {
    }
    FilterIterator<UpperIterator, FilterFields, Fields...> &operator=(FilterIterator<UpperIterator, FilterFields, Fields...> &&other) = default;
    FilterIterator<UpperIterator, FilterFields, Fields...> &operator=(const FilterIterator<UpperIterator, FilterFields, Fields...> &other) = default;
    explicit operator bool() const noexcept
    {
        while (_iter)
        {
            if (apply(_func, _iter.value(), IndexType{}))
            {
                return true;
            }
            _iter.next();
        }
        return false;
    }

    void next()
    {
        do
        {
            _iter.next();
        } while (!apply(_func, _iter.value(), IndexType{}) && _iter);
    }

    value_type value()
    {
        return _iter.value();
    }

    value_type value() const
    {
        return _iter.value();
    }
};

template <typename UpStream, typename FilterFields, typename... Fields>
class FilterStream : public Stream<FilterStream<UpStream, FilterFields, Fields...>, FilterIterator<typename UpStream::Iterator, FilterFields, Fields...>, Fields...>
{
public:
    using Iterator = FilterIterator<typename UpStream::Iterator, FilterFields, Fields...>;
    using func_type = typename Iterator::func;

private:
    UpStream _up;
    func_type _func;

public:
    Iterator iterator_impl()
    {
        return Iterator(_up.iterator(), _func);
    }

public:
    FilterStream(UpStream &&up, func_type func) : _up(up), _func(func)
    {
    }
    FilterStream(const UpStream &up, func_type func) : _up(up), _func(func)
    {
    }

    FilterStream(const FilterStream<UpStream, FilterFields, Fields...> &other) = default;
    FilterStream(FilterStream<UpStream, FilterFields, Fields...> &&other) = default;
    FilterStream<UpStream, FilterFields, Fields...> &operator=(const FilterStream<UpStream, FilterFields, Fields...> &other) = default;
    FilterStream<UpStream, FilterFields, Fields...> &operator=(FilterStream<UpStream, FilterFields, Fields...> &&other) = default;
};

template <typename... Fields>
class MemoryTable
{
public:
    using __Checker = std::tuple<CheckField<Fields>...>;

public:
    using value_type = std::tuple<typename Fields::type...>;

    MemoryTable()
    {
    }
    MemoryTable(MemoryTable<Fields...> &&other) = default;
    MemoryTable(const MemoryTable<Fields...> &other) = default;
    MemoryTable(std::initializer_list<value_type> l) : _details(l)
    {
    }
    ~MemoryTable() = default;

    MemoryTable<Fields...> &operator=(MemoryTable &&other) = default;
    MemoryTable<Fields...> &operator=(const MemoryTable &other) = default;
    void emplace(const typename Fields::type &... args)
    {
        _details.emplace_back(args...);
    }

    void push(const value_type &value)
    {
        _details.push_back(value);
    }

    template <typename... Args>
    void emplace_forward(Args &&... args)
    {
        _details.emplace_back(std::forward<Args>(args)...);
    }

    MemoryTableStream<Fields...> stream() const
    {
        return MemoryTableStream<Fields...>(this);
    }

    template <typename... Fields2>
    friend class MemoryTableStream;

    template <typename UpStream, typename SortFields, typename... Fields2>
    friend class SortStream;

private:
    std::deque<value_type> _details;
};

template<typename State, typename Fields>
struct GeneratorTraits;

template<typename State, typename...Fields>
struct GeneratorTraits <State, std::tuple<Fields...>> {
    using type = Generator<State, Fields...>;
};

template<typename Stream1, typename Stream2>
struct CombineStreamState {
    Stream1 stream1;
    Stream2 stream2;
    typename Stream1::Iterator iter_a;
    typename Stream2::Iterator iter_b;
};
template<typename Stream1, typename Stream2>
using CombineGenerator = typename GeneratorTraits<CombineStreamState<Stream1, Stream2>, typename CombineTuple<typename Stream1::field_types, typename Stream2::field_types>::type>::type;

template<typename Stream1, typename Stream2>
auto combine_stream(Stream1&& stream1, Stream2&& stream2) -> CombineGenerator<Stream1, Stream2> {
    using value_type = typename CombineGenerator<Stream1, Stream2>::value_type;
    using state_type = CombineStreamState<Stream1, Stream2>;
    return CombineGenerator<Stream1, Stream2>([](state_type & state) -> std::pair<bool, value_type> {
        while (state.iter_a) {
            if (state.iter_b) {
                auto res = std::tuple_cat(state.iter_a.value(), state.iter_b.value());
                state.iter_b.next();
                return {true, res};
            }
            state.iter_a.next();
            state.iter_b = state.stream2.iterator();
            if (!state.iter_b) {
                return {false, {}};
            }
        }
        return {false, {}};
    }, [&stream1, &stream2]() -> state_type {
        return {stream1, stream2, stream1.iterator(), stream2.iterator()};
    });
}

#endif //MEM_TABLE_H