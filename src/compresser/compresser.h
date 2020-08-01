#ifndef __GZIP_H__
#define __GZIP_H__

#include <sstream>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>

std::string compress(const std::string& data);
std::string decompress(const std::string& data);

#endif // __GZIP_H__
