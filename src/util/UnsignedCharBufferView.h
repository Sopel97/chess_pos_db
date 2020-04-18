#pragma once

#include <string_view>

namespace util
{
    struct UnsignedCharBufferView : public std::basic_string_view<unsigned char>
    {
        using BaseType = std::basic_string_view<unsigned char>;
        
        using BaseType::BaseType;

        [[nodiscard]] constexpr static UnsignedCharBufferView fromStringView(std::string_view sv)
        {
            return UnsignedCharBufferView(reinterpret_cast<const unsigned char*>(sv.data()), sv.size());
        }

        [[nodiscard]] constexpr std::string_view toStringView() const
        {
            return std::string_view(reinterpret_cast<const char*>(data()), size());
        }

        [[nodiscard]] constexpr UnsignedCharBufferView substr(size_type pos, size_type count = npos) const
        {
            return UnsignedCharBufferView(data() + pos, count);
        }
    };
}
