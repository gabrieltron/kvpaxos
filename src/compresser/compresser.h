// Copyright 2007 Timo Bingmann <tb@panthema.net>
// Distributed under the Boost Software License, Version 1.0.
// (See http://www.boost.org/LICENSE_1_0.txt)
//
// Original link http://panthema.net/2007/0328-ZLibString.html

#ifndef COMPRESSER_H
#define COMPRESSER_H

#include <string>
#include <string.h>
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <sstream>

#include <zlib.h>

#include "constants/constants.h"

/** Compress a STL string using zlib with given compression level and return
  * the binary data. */
std::string compress(const std::string& str,
                            int compressionlevel = Z_BEST_COMPRESSION);

/** Decompress an STL string using zlib and return the original data. */
std::string decompress(const std::string& str);

#endif
