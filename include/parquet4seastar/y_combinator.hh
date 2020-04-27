/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
  This is an implementation of a fixed-point combinator.
  It is used to make recursive lambdas.
  This enables the use of lambdas as helper functions for traversing trees.
  We use it for traversing schema trees.
 */

#pragma once

namespace parquet4seastar {

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

} // namespace parquet4seastar
