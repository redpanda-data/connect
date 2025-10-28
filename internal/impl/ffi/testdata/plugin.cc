// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <mutex>
#include <stdint.h>

// Compile via on linux:
// ```
// g++ -shared -fPIC plugin.cc -o plugin.so
// ```
// Or on darwin:
// ```
// clang++ -shared -fPIC plugin.cc -o plugin.dylib
// ```

extern "C" int ReverseBytes(void *input, void *output, int32_t len) {
  auto *src = static_cast<char *>(input);
  auto *dest = static_cast<char *>(output);
  std::reverse_copy(src, src + len, dest);
  return len;
}

static int64_t GLOBAL_STATE = 0;
static std::mutex GLOBAL_STATE_MU;

extern "C" void SetState(int64_t v) {
  std::lock_guard<std::mutex> l(GLOBAL_STATE_MU);
  GLOBAL_STATE = v;
}

extern "C" int64_t GetState() {
  std::lock_guard<std::mutex> l(GLOBAL_STATE_MU);
  return GLOBAL_STATE;
}

extern "C" int32_t UpperBits(int64_t v) {
  return static_cast<int32_t>(v >> 32);
}
