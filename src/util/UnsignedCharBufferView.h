#pragma once

#include <string_view>

namespace util
{
    struct UnsignedCharBufferView : public std::basic_string_view<unsigned char>
    {
        using BaseType = std::basic_string_view<unsigned char>;
        
        using BaseType::BaseType;

        [[nodiscard]] static UnsignedCharBufferView fromStringView(std::string_view sv)
        {
            return UnsignedCharBufferView(reinterpret_cast<const unsigned char*>(sv.data()), sv.size());
        }

        [[nodiscard]] std::string_view toStringView() const
        {
            return std::string_view(reinterpret_cast<const char*>(data()), size());
        }

        [[nodiscard]] constexpr UnsignedCharBufferView substr(size_type pos, size_type count = npos) const
        {
            const auto s = count > size() - pos ? size() - pos : count;
            return UnsignedCharBufferView(data() + pos, s);
        }
    };
}
