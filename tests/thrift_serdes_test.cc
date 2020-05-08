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

#define BOOST_TEST_MODULE parquet

#include <parquet4seastar/thrift_serdes.hh>
#include <parquet4seastar/parquet_types.h>
#include <boost/test/included/unit_test.hpp>

BOOST_AUTO_TEST_CASE(thrift_serdes) {
    using namespace parquet4seastar;
    thrift_serializer serializer;

    format::SchemaElement se;
    se.__set_type(format::Type::DOUBLE);
    format::FileMetaData fmd;
    fmd.__set_schema({se});
    auto serialized = serializer.serialize(fmd);

    format::FileMetaData fmd2;
    deserialize_thrift_msg(serialized.data(), serialized.size(), fmd2);

    BOOST_CHECK(fmd2.schema[0].type == format::Type::DOUBLE);
}
