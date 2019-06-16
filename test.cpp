#include <iostream>
#include <string>
#include <utility>

#include "mem_table.hpp"

DEF_Field("name", std::string, HEAD_name);
DEF_Field("name2", std::string, HEAD_name2);
struct value
{
    int id;
    int xx;
};
DEF_Field("zzzz", value, HEAD_zzz);

DEF_Field("count", size_t, HEAD_count);

std::ostream &operator<<(std::ostream &os, const value &dt)
{
    os << '{' << dt.id << ", " << dt.xx << '}';
    return os;
}

int main()
{
    make_generator<std::pair<size_t, std::string>, HEAD_name>([](std::pair<size_t, std::string> &state) -> std::pair<bool, std::string> {
        if (state.first >= state.second.length()) {
            return {false, ""};
        } else {
            return {true, std::string(1, state.second[state.first++])};
        } }, []() { return std::pair<size_t, std::string>{0, "hello world"}; }).for_each([](const std::string &value) {
        std::cout << value << std::endl;
    });

    auto a = MemoryTable<HEAD_name, HEAD_zzz, HEAD_name2>::make({{"jack", {1, 2}, "jack"}, {"london", {-1, 4}, "london2"}});
    value tmp{2, 3};
    a->emplace_forward("jack", std::cref(tmp), "london");
    a->emplace_forward("jack", std::ref(tmp), "london");
    a->emplace_forward("jack", value{5, 4}, "london");
    //a.emplace_forward("jack", value{5, 4}, "london");

    (*a)->select<HEAD_name>().aggerate_by<std::tuple<HEAD_name>, size_t, HEAD_name, HEAD_count>([](const std::tuple<std::string> &key, size_t &value, const std::string &name) -> void { value++; },
                                                                                                     [](const std::tuple<std::string> &key) -> size_t { return 0; },
                                                                                                     [](const std::tuple<std::string> &key, const size_t &finish) -> std::tuple<std::string, size_t> { return {std::get<0>(key), finish}; })
                                  .for_each([](const std::string& name, const size_t& count) -> void {
                                        std::cout << name << " has: " << count << std::endl;
                                  });

    int i = 0;
    combine_stream((*a)->select<HEAD_name>(), (*a)->select<HEAD_name>())
        .for_each([&i](const std::string &x, const std::string &y) -> void {
            if (x == y)
            {
                std::cout << (i++) << ": " << x << ": " << y << std::endl;
            }
        });

    HashMemoryTableIndex<std::tuple<HEAD_name>, HEAD_name, HEAD_zzz, HEAD_name2> index;
    index.reset(a);

    auto manager = std::make_shared<IndexManager<std::tuple<>, HEAD_name, HEAD_zzz, HEAD_name2>>();
    auto new_manager = manager->create_index<HEAD_name>()->create_index<HEAD_name2>();
    std::cout << __LINE__ << std::endl;
    //new_manager->create_index<>();
    std::cout<< new_manager->debug() <<std::endl;

    auto iter = index.find("jack");
    std::cout << "begin: " << std::endl;
    for (auto it = iter.first; it != iter.second; it++) {
        std::cout << it->second << std::endl;
    }
    std::cout << "end: " << std::endl;

    std::cout << (*a)->limit(2).collect()->stream().count() << std::endl;
    a->stream().for_each([](const std::string &name, const value &val, const std::string &val2) -> void {
        std::cout << name << val.id << ": " << val.xx << std::endl;
    });
    auto x = a->stream().limit(4);
    auto y = x.select<HEAD_name, HEAD_zzz>().order_by<HEAD_name, HEAD_name>();

    std::cout << y.skip(2).skip(3).limit(4).skip(4).debug() << std::endl;

    y.filter_by<HEAD_name>([](const std::string &value) -> bool {
         return value == "london1";
     })
        .for_each([](const std::string &name, const value &value) -> void {
            std::cout << name << ": " << value << std::endl;
        });
    return 0;
}
