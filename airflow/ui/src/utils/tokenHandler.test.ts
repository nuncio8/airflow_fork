/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import type { InternalAxiosRequestConfig } from "axios";
import { afterEach, describe, it, vi, expect } from "vitest";

import { TOKEN_QUERY_PARAM_NAME, TOKEN_STORAGE_KEY, tokenHandler } from "./tokenHandler";

describe.each([
  { searchParams: new URLSearchParams({ token: "something" }) },
  { searchParams: new URLSearchParams({ param2: "someParam2", token: "else" }) },
  { searchParams: new URLSearchParams({}) },
])("TokenFlow Interceptor", ({ searchParams }) => {
  it("Should read from the SearchParams, persist to the localStorage and remove from the SearchParams", () => {
    const token = searchParams.get(TOKEN_QUERY_PARAM_NAME);

    const setItemMock = vi.spyOn(localStorage, "setItem");

    vi.stubGlobal("location", { search: searchParams.toString() });

    const headers = {};

    const config = { headers };

    const { headers: updatedHeaders } = tokenHandler(config as InternalAxiosRequestConfig);

    if (token === null) {
      expect(setItemMock).toHaveBeenCalledTimes(0);
    } else {
      expect(setItemMock).toHaveBeenCalledOnce();
      expect(setItemMock).toHaveBeenCalledWith(TOKEN_STORAGE_KEY, token);
      expect(searchParams).not.to.contains.keys(TOKEN_QUERY_PARAM_NAME);
      expect(updatedHeaders).toEqual({ Authorization: `Bearer ${token}` });
    }
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
