#pragma once

namespace parquet {

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

template <class F>
struct y_combinator {
    F f;
    template <class... Args>
    decltype(auto) operator()(Args&&... args) {
        return f(*this, std::forward<Args>(args)...);
    }
    template <class... Args>
    decltype(auto) operator()(Args&&... args) const {
        return f(*this, std::forward<Args>(args)...);
    }
};
template <class F> y_combinator(F) -> y_combinator<F>;

} // namespace
