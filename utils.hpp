#include<tuple>
#include<type_traits>

template <template <typename> class Func, typename... Args>
struct ForEach
{
    typedef int type;
};

template <template <typename> class Func, typename A, typename... Args>
struct ForEach<Func, A, Args...> : ForEach<Func, Args...>, Func<A>
{
};

template <typename Tuple>
struct TupleTail
{
    using type = std::tuple<>;
};
template <typename Head, typename... Tail>
struct TupleTail<std::tuple<Head, Tail...>>
{
    using type = std::tuple<Tail...>;
};

static_assert(std::is_same<TupleTail<std::tuple<>>::type, std::tuple<>>::value, "");
static_assert(std::is_same<TupleTail<std::tuple<int>>::type, std::tuple<>>::value, "");
static_assert(std::is_same<TupleTail<std::tuple<float,int,bool>>::type, std::tuple<int, bool>>::value, "");

template <typename Type, typename Tuple, size_t count>
struct FindTypeIndexHelper : FindTypeIndexHelper<Type, typename TupleTail<Tuple>::type, count + 1>
{
};

template <typename Type, size_t count, typename... Args>
struct FindTypeIndexHelper<Type, std::tuple<Type, Args...>, count>
{
    constexpr static size_t value = count;
};

template <typename Type, size_t count>
struct FindTypeIndexHelper<Type, std::tuple<>, count>
{
    constexpr static size_t value = count;
};

template <typename Type, typename... Types>
using FindTypeIndex = FindTypeIndexHelper<Type, std::tuple<Types...>, 0>;

static_assert(FindTypeIndex<int, int, int, float>::value==0, "");
static_assert(FindTypeIndex<int, float, float, int>::value==2, "");
static_assert(FindTypeIndex<int>::value==0, "");
static_assert(FindTypeIndex<int, float>::value==1, "");

template <size_t... I>
struct IndexType
{
};

template <class IndexTypeA, size_t B>
struct CombineIndex;

template <size_t B, size_t... I>
struct CombineIndex<IndexType<I...>, B>
{
    using type = IndexType<I..., B>;
};

static_assert(std::is_same<CombineIndex<IndexType<>, 2>::type, IndexType<2>>::value, "");
static_assert(std::is_same<CombineIndex<IndexType<1,3>, 2>::type, IndexType<1,3,2>>::value, "");

template <size_t N>
struct IndexGen : CombineIndex<typename IndexGen<N - 1>::type, N - 1>
{
};
template <>
struct IndexGen<0>
{
    using type = IndexType<>;
};

static_assert(std::is_same<IndexGen<0>::type, IndexType<>>::value, "");
static_assert(std::is_same<IndexGen<2>::type, IndexType<0,1>>::value, "");

template <template <typename, typename> class Func, typename Init, typename... Args>
struct Reduce
{
    using type = Init;
};

template <template <typename, typename> class Func, typename Init, typename A, typename... Args>
struct Reduce<Func, Init, A, Args...> : Reduce<Func, typename Func<Init, A>::type, Args...>
{
};

template<size_t N>
struct IntegerConstant {
    constexpr static size_t value = N;
};

template<typename A, typename B>
struct AddIntegerConstant {
    using type = IntegerConstant<A::value + B::value>;
};

static_assert(std::is_same<AddIntegerConstant<IntegerConstant<33>, IntegerConstant<22>>::type, IntegerConstant<55>>::value, "");

using IConstZero = IntegerConstant<0>;
using IConstOne = IntegerConstant<1>;

static_assert(std::is_same<Reduce<AddIntegerConstant, IConstZero>::type, IConstZero>::value, "");
static_assert(std::is_same<Reduce<AddIntegerConstant, IConstZero, IConstZero>::type, IConstZero>::value, "");
static_assert(std::is_same<Reduce<AddIntegerConstant, IConstOne, IConstOne, IntegerConstant<2>>::type, IntegerConstant<4>>::value, "");

template <template <typename, typename> class Func, typename Init, typename Tuple>
struct ReduceTuple
{
};

template <template <typename, typename> class Func, typename Init, typename... Args>
struct ReduceTuple<Func, Init, std::tuple<Args...>> : Reduce<Func, Init, Args...>
{
};

static_assert(std::is_same<ReduceTuple<AddIntegerConstant, IConstZero, std::tuple<>>::type, IConstZero>::value, "");
static_assert(std::is_same<ReduceTuple<AddIntegerConstant, IConstZero, std::tuple<IConstZero>>::type, IConstZero>::value, "");
static_assert(std::is_same<ReduceTuple<AddIntegerConstant, IConstOne, std::tuple<IConstOne, IntegerConstant<2>>>::type, IntegerConstant<4>>::value, "");

template <typename Tuple, typename Type>
struct TypeIndexInTuple
{
};

template <typename Type, typename... Args>
struct TypeIndexInTuple<std::tuple<Args...>, Type>
{
    constexpr static size_t value = FindTypeIndex<Type, Args...>::value;
};

static_assert(TypeIndexInTuple<std::tuple<int, int, float>, int>::value==0, "");
static_assert(TypeIndexInTuple<std::tuple<float, float, int>, int>::value==2, "");
static_assert(TypeIndexInTuple<std::tuple<>, int>::value==0, "");
static_assert(TypeIndexInTuple<std::tuple<float>, int>::value==1, "");

template <typename A, template <typename, typename...> class Func>
struct Fold
{
    template <typename... B>
    using func = Func<A, B...>;
};
template <template <typename> class Func1, template <typename> class Func2>
struct Bind
{
    template <typename A>
    using func = Func1<typename Func2<A>::type>;
};

template <typename Tuple>
struct FindAndAcc
{
    template <typename Init, typename Type>
    using func = CombineIndex<Init, TypeIndexInTuple<Tuple, Type>::value>;
};

template <typename Tuple, typename... Types>
using TypeIndicesInTuple = Reduce<FindAndAcc<Tuple>::template func, IndexType<>, Types...>;

template <typename Tuple, typename Types>
struct TupleTypeIndicesInTypesTraits;
template <typename Tuple, typename... Types>
struct TupleTypeIndicesInTypesTraits<std::tuple<Types...>, Tuple>
{
    using type = typename TypeIndicesInTuple<Tuple, Types...>::type;
};
template <typename Tuple, typename... Types>
using TupleTypeIndicesInTypes = TupleTypeIndicesInTypesTraits<Tuple, std::tuple<Types...>>;

static_assert(std::is_same<TupleTypeIndicesInTypes<std::tuple<int, float, double>, float, int, std::tuple<>>::type, IndexType<1, 0, 3>>::value, "");

template <typename FUNC, typename Tuple, size_t... I>
auto apply(FUNC &&func, Tuple &&tuple, IndexType<I...>)
    -> decltype(func(std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(tuple))...))
{
    return func(std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(tuple))...);
}

template <typename FUNC, typename Arg0, typename Tuple, size_t... I>
auto apply_ex(FUNC &&func, Arg0 &&arg, Tuple &&tuple, IndexType<I...>)
    -> decltype(func(std::forward<Arg0>(arg), std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(tuple))...))
{
    return func(std::forward<Arg0>(arg), std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(tuple))...);
}

template <typename FUNC, typename Tuple>
auto apply_all(FUNC &&func, Tuple &&tuple)
    -> decltype(apply(std::forward<FUNC>(func), std::forward<Tuple>(tuple), typename IndexGen<std::tuple_size<typename std::remove_reference<Tuple>::type>::value>::type{}))
{
    using IndexT = typename IndexGen<std::tuple_size<typename std::remove_reference<Tuple>::type>::value>::type;
    return apply(std::forward<FUNC>(func), std::forward<Tuple>(tuple), IndexT{});
}

template <typename FUNC, typename Type, typename Tuple>
auto apply_all_ex(FUNC &&func, Type &&v, Tuple &&tuple)
    -> decltype(apply_ex(std::forward<FUNC>(func), std::forward<Type>(v), std::forward<Tuple>(tuple), typename IndexGen<std::tuple_size<typename std::remove_reference<Tuple>::type>::value>::type{}))
{
    using IndexT = typename IndexGen<std::tuple_size<typename std::remove_reference<Tuple>::type>::value>::type;
    return apply_ex(std::forward<FUNC>(func), std::forward<Type>(v), std::forward<Tuple>(tuple), IndexT{});
}

template <typename FUNC, typename Tuple, size_t... I>
auto apply(FUNC &&func, Tuple &&a, Tuple &&b, IndexType<I...>)
    -> decltype(func(std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(a))..., std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(b))...))
{
    return func(std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(a))...,
                std::forward<typename std::tuple_element<I, typename std::remove_reference<Tuple>::type>::type>(std::get<I>(b))...);
}
template<typename Tuple1, typename Tuple2>
struct CombineTuple2 {
    using type = decltype(std::tuple_cat(Tuple1(), Tuple2()));
};

template<typename...Tuples>
using CombineTuple = Reduce<CombineTuple2, std::tuple<>, Tuples...>;

static_assert(std::is_same<typename CombineTuple<std::tuple<>, std::tuple<>>::type, std::tuple<>>::value, "");
static_assert(std::is_same<typename CombineTuple<std::tuple<int>, std::tuple<int>>::type, std::tuple<int, int>>::value, "");