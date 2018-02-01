// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "strtol.h"

#include <climits>
#include <cmath>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <tuple>

using std::ostringstream;
template <class T>
using opt_tuple = std::tuple<std::optional<T>, std::optional<std::string>>;

opt_tuple<long long> strict_strtoll(const std::string_view str, int base)
{
  ostringstream err;
  if (auto invalid = str.find_first_not_of("0123456789-+");
      invalid != std::string_view::npos ||
      invalid == 0) {
    err << "The option value '" << str << "' contains invalid digits";
    return std::tuple{std::optional<long long>(), err.str()};
  }
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  long long ret = strtoll(str.data(), &endptr, base);

  if (endptr == str.data()) {
    err << "Expected option value to be integer, got '" << str << "'";
    return std::tuple{std::optional<long long>(), err.str()};
  }
  if ((errno == ERANGE && (ret == LLONG_MAX || ret == LLONG_MIN))
      || (errno != 0 && ret == 0)) {
    err << "The option value '" << str << "' seems to be invalid";
    return std::tuple{std::optional<long long>(), err.str()};
  }
  return std::tuple{ret, std::optional<std::string>()};
}

opt_tuple<long long> strict_strtoll(const char *str, int base)
{
  return strict_strtoll(std::string_view(str), base);
}

long long strict_strtoll(const char *str, int base, std::string *err)
{
  opt_tuple<long long> ret = strict_strtoll(std::string_view(str), base);
  if (std::get<0>(ret))
    return *std::get<0>(ret);
  else {
    *err = *std::get<1>(ret);
    return 0;
  }
}

opt_tuple<int> strict_strtol(std::string_view str, int base)
{
  ostringstream err;
  auto ret = strict_strtoll(str, base);
  auto val = std::get<0>(ret);
  if (!val)
    return std::tuple{std::optional<int>(), std::get<1>(ret)};
  if ((*val < INT_MIN) || (*val > INT_MAX)) {
    err << "The option value '" << str << "' seems to be invalid";
    return std::tuple{std::optional<int>(), err.str()};
  }
  return std::tuple{static_cast<int>(*val), std::optional<std::string>()};
}
int strict_strtol(const char *str, int base, std::string *err)
{
  opt_tuple<int> ret = strict_strtol(std::string_view(str), base);
  if (std::get<0>(ret))
    return *std::get<0>(ret);
  else {
    *err = *std::get<1>(ret);
    return 0;
  }
}

opt_tuple<int> strict_strtol(char *str, int base)
{
  return strict_strtol(std::string_view(str), base);
}

double strict_strtod(const std::string_view str, std::string *err)
{
  char *endptr;
  ostringstream oss;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  double ret = strtod(str.data(), &endptr);
  if (errno == ERANGE) {
    oss << "strict_strtod: floating point overflow or underflow parsing '"
	<< str << "'";
    *err = oss.str();
    return 0.0;
  }
  if (endptr == str) {
    oss << "strict_strtod: expected double, got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (*endptr != '\0') {
    oss << "strict_strtod: garbage at end of string. got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  *err = "";
  return ret;
}

double strict_strtod(const char *str, std::string *err)
{
  return strict_strtod(std::string_view(str), err);
}

float strict_strtof(const std::string_view str, std::string *err)
{
  char *endptr;
  ostringstream oss;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  float ret = strtof(str.data(), &endptr);
  if (errno == ERANGE) {
    oss << "strict_strtof: floating point overflow or underflow parsing '"
	<< str << "'";
    *err = oss.str();
    return 0.0;
  }
  if (endptr == str) {
    oss << "strict_strtof: expected float, got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (*endptr != '\0') {
    oss << "strict_strtof: garbage at end of string. got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  *err = "";
  return ret;
}

float strict_strtof(const char *str, std::string *err)
{
  return strict_strtof(std::string_view(str), err);
}

template<typename T>
opt_tuple<T> strict_iec_cast(const std::string_view str)
{
  ostringstream err;
  if (str.empty()) {
    err << "strict_iecstrtoll: value not specified";
    return std::tuple{std::optional<long long>(), err.str()};
  }
  // get a view of the unit and of the value
  std::string_view unit;
  std::string_view n = str;
  size_t u = str.find_first_not_of("0123456789-+");
  int m = 0;
  // deal with unit prefix is there is one
  if (u != std::string_view::npos) {
    n = str.substr(0, u);
    unit = str.substr(u, str.length() - u);
    // we accept both old si prefixes as well as the proper iec prefixes
    // i.e. K, M, ... and Ki, Mi, ...
    if (unit.back() == 'i') {
      if (unit.front() == 'B') {
        err << "strict_iecstrtoll: illegal prefix \"Bi\"";
        return std::tuple{std::optional<long long>(), err.str()};
      }
    }
    if (unit.length() > 2) {
      err << "strict_iecstrtoll: illegal prefix (length > 2)";
      return std::tuple{std::optional<long long>(), err.str()};
    }
    if (unit.front() == 'K')
      m = 10;
    else if (unit.front() == 'M')
      m = 20;
    else if (unit.front() == 'G')
      m = 30;
    else if (unit.front() == 'T')
      m = 40;
    else if (unit.front() == 'P')
      m = 50;
    else if (unit.front() == 'E')
      m = 60;
    else if (unit.front() != 'B') {
      err << "strict_iecstrtoll: unit prefix not recognized";
      return std::tuple{std::optional<long long>(), err.str()};
    }
  }

  auto to_ll = strict_strtoll(n, 10);
  auto ll = std::get<0>(to_ll);
  if (!ll)
    return to_ll;

  if (*ll < 0 && !std::numeric_limits<T>::is_signed) {
    err << "strict_iecstrtoll: value should not be negative";
    return std::tuple{std::optional<long long>(), err.str()};
  }
  if (static_cast<unsigned>(m) >= sizeof(T) * CHAR_BIT) {
    err << ("strict_iecstrtoll: the IEC prefix is too large for the designated "
        "type");
    return std::tuple{std::optional<long long>(), err.str()};
  }
  using promoted_t = typename std::common_type<decltype(*ll), T>::type;
  if (static_cast<promoted_t>(*ll) <
      static_cast<promoted_t>(std::numeric_limits<T>::min()) >> m) {
    err << "strict_iecstrtoll: value seems to be too small";
    return std::tuple{std::optional<long long>(), err.str()};
  }
  if (static_cast<promoted_t>(*ll) >
      static_cast<promoted_t>(std::numeric_limits<T>::max()) >> m) {
    err << "strict_iecstrtoll: value seems to be too large";
    return std::tuple{std::optional<long long>(), err.str()};
  }
  return std::tuple{(*ll << m), std::optional<std::string>()};
}

template opt_tuple<int> strict_iec_cast<int>(const std::string_view str);
template opt_tuple<long> strict_iec_cast<long>(const std::string_view str);
template opt_tuple<long long> strict_iec_cast<long long>(const std::string_view str);
template opt_tuple<uint64_t> strict_iec_cast<uint64_t>(const std::string_view str);
template opt_tuple<uint32_t> strict_iec_cast<uint32_t>(const std::string_view str);

opt_tuple<uint64_t> strict_iecstrtoll(const std::string_view str)
{
  return strict_iec_cast<uint64_t>(str);
}

opt_tuple<uint64_t> strict_iecstrtoll(const char *str)
{
  return strict_iec_cast<uint64_t>(std::string_view(str));
}

template<typename T>
opt_tuple<T> strict_iec_cast(const char *str)
{
  return strict_iec_cast<T>(std::string_view(str));
}

template opt_tuple<int> strict_iec_cast<int>(const char *str);
template opt_tuple<long> strict_iec_cast<long>(const char *str);
template opt_tuple<long long> strict_iec_cast<long long>(const char *str);
template opt_tuple<uint64_t> strict_iec_cast<uint64_t>(const char *str);
template opt_tuple<uint32_t> strict_iec_cast<uint32_t>(const char *str);

template<typename T>
opt_tuple<T> strict_si_cast(const std::string_view str)
{
  ostringstream err;
  if (str.empty()) {
    err << "strict_sistrtoll: value not specified";
    return std::tuple{std::optional<T>(), err.str()};
  }
  std::string_view n = str;
  int m = 0;
  // deal with unit prefix is there is one
  if (str.find_first_not_of("0123456789+-") != std::string_view::npos) {
    const char &u = str.back();
    if (u == 'K')
      m = 3;
    else if (u == 'M')
      m = 6;
    else if (u == 'G')
      m = 9;
    else if (u == 'T')
      m = 12;
    else if (u == 'P')
      m = 15;
    else if (u == 'E')
      m = 18;
    else if (u != 'B') {
      err << "strict_si_cast: unit prefix not recognized";
      return std::tuple{std::optional<T>(), err.str()};
    }

    if (m >= 3)
      n = str.substr(0, str.length() -1);
  }

  auto to_ll = strict_strtoll(n, 10);
  auto ll = std::get<0>(to_ll);
  // return if strict_strtoll returns an error
  if (!ll)
    return to_ll;

  if (*ll < 0 && !std::numeric_limits<T>::is_signed) {
    err << "strict_sistrtoll: value should not be negative";
    return std::tuple{std::optional<T>(), err.str()};
  }
  using promoted_t = typename std::common_type<decltype(*ll), T>::type;
  if (static_cast<promoted_t>(*ll) <
      static_cast<promoted_t>(std::numeric_limits<T>::min()) / pow (10, m)) {
    err << "strict_sistrtoll: value seems to be too small";
    return std::tuple{std::optional<T>(), err.str()};
  }
  if (static_cast<promoted_t>(*ll) >
      static_cast<promoted_t>(std::numeric_limits<T>::max()) / pow (10, m)) {
    err << "strict_sistrtoll: value seems to be too large";
    return std::tuple{std::optional<T>(), err.str()};
  }
  return std::tuple{(*ll * pow (10,  m)), std::optional<std::string>()};
}

template opt_tuple<int> strict_si_cast<int>(const std::string_view str);
template opt_tuple<long> strict_si_cast<long>(const std::string_view str);
template opt_tuple<long long> strict_si_cast<long long>(const std::string_view str);
template opt_tuple<uint64_t> strict_si_cast<uint64_t>(const std::string_view str);
template opt_tuple<uint32_t> strict_si_cast<uint32_t>(const std::string_view str);

opt_tuple<uint64_t> strict_sistrtoll(const std::string_view str)
{
  return strict_si_cast<uint64_t>(str);
}

opt_tuple<uint64_t> strict_sistrtoll(const char *str)
{
  return strict_si_cast<uint64_t>(str);
}

template<typename T>
opt_tuple<T> strict_si_cast(const char *str)
{
  return strict_si_cast<T>(std::string_view(str));
}

template opt_tuple<int> strict_si_cast<int>(const char *str);
template opt_tuple<long> strict_si_cast<long>(const char *str);
template opt_tuple<long long> strict_si_cast<long long>(const char *str);
template opt_tuple<uint64_t> strict_si_cast<uint64_t>(const char *str);
template opt_tuple<uint32_t> strict_si_cast<uint32_t>(const char *str);
