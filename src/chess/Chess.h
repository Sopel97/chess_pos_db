#pragma once

#include "enum/Enum.h"
#include "enum/EnumArray.h"

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

enum struct Color : std::uint8_t
{
    White,
    Black
};

template <>
struct EnumTraits<Color>
{
    using IdType = int;
    using EnumType = Color;

    static constexpr int cardinality = 2;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        Color::White,
        Color::Black
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality);

        return static_cast<EnumType>(id);
    }

    [[nodiscard]] static constexpr std::string_view toString(EnumType c) noexcept
    {
        return std::string_view("wb" + ordinal(c), 1);
    }

    [[nodiscard]] static constexpr char toChar(EnumType c) noexcept
    {
        return "wb"[ordinal(c)];
    }

    [[nodiscard]] static constexpr std::optional<Color> fromChar(char c) noexcept
    {
        if (c == 'w') return Color::White;
        if (c == 'b') return Color::Black;

        return {};
    }

    [[nodiscard]] static constexpr std::optional<Color> fromString(std::string_view sv) noexcept
    {
        if (sv.size() != 1) return {};

        return fromChar(sv[0]);
    }
};

constexpr Color operator!(Color c)
{
    return fromOrdinal<Color>(ordinal(c) ^ 1);
}

enum struct PieceType : std::uint8_t
{
    Pawn,
    Knight,
    Bishop,
    Rook,
    Queen,
    King,

    None
};

template <>
struct EnumTraits<PieceType>
{
    using IdType = int;
    using EnumType = PieceType;

    static constexpr int cardinality = 7;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        PieceType::Pawn,
        PieceType::Knight,
        PieceType::Bishop,
        PieceType::Rook,
        PieceType::Queen,
        PieceType::King,
        PieceType::None
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality);

        return static_cast<EnumType>(id);
    }

    [[nodiscard]] static constexpr std::string_view toString(EnumType p, Color c) noexcept
    {
        return std::string_view("PpNnBbRrQqKk " + (ordinal(p) * 2 + ::ordinal(c)), 1);
    }

    [[nodiscard]] static constexpr char toChar(EnumType p, Color c) noexcept
    {
        return "PpNnBbRrQqKk "[ordinal(p) * 2 + ::ordinal(c)];
    }

    [[nodiscard]] static constexpr std::optional<PieceType> fromChar(char c) noexcept
    {
        auto it = std::string_view("PpNnBbRrQqKk ").find(c);
        if (it == std::string::npos) return {};
        else return static_cast<PieceType>(it/2);
    }

    [[nodiscard]] static constexpr std::optional<PieceType> fromString(std::string_view sv) noexcept
    {
        if (sv.size() != 1) return {};

        return fromChar(sv[0]);
    }
};

struct Piece
{
    [[nodiscard]] static constexpr Piece fromId(int id)
    {
        return Piece(id);
    }

    [[nodiscard]] static constexpr Piece none()
    {
        return Piece(PieceType::None, Color::White);
    }

    constexpr Piece() noexcept :
        Piece(PieceType::None, Color::White)
    {

    }

    constexpr Piece(PieceType type, Color color) noexcept :
        m_id((ordinal(type) << 1) | ordinal(color))
    {
        ASSERT(type != PieceType::None || color == Color::White);
    }

    constexpr Piece& operator=(const Piece& other) = default;

    [[nodiscard]] constexpr friend bool operator==(Piece lhs, Piece rhs) noexcept
    {
        return lhs.m_id == rhs.m_id;
    }

    [[nodiscard]] constexpr friend bool operator!=(Piece lhs, Piece rhs) noexcept
    {
        return !(lhs == rhs);
    }

    [[nodiscard]] constexpr PieceType type() const
    {
        return fromOrdinal<PieceType>(m_id >> 1);
    }

    [[nodiscard]] constexpr Color color() const
    {
        return fromOrdinal<Color>(m_id & 1);
    }

    [[nodiscard]] constexpr std::pair<PieceType, Color> parts() const
    {
        return std::make_pair(type(), color());
    }

    [[nodiscard]] constexpr explicit operator int() const
    {
        return static_cast<int>(m_id);
    }

private:
    constexpr Piece(int id) :
        m_id(id)
    {
    }

    std::uint8_t m_id; // lowest bit is a color, 7 highest bits are a piece type
};

[[nodiscard]] constexpr Piece operator|(PieceType type, Color color) noexcept
{
    return Piece(type, color);
}

[[nodiscard]] constexpr Piece operator|(Color color, PieceType type) noexcept
{
    return Piece(type, color);
}

constexpr Piece whitePawn = Piece(PieceType::Pawn, Color::White);
constexpr Piece whiteKnight = Piece(PieceType::Knight, Color::White);
constexpr Piece whiteBishop = Piece(PieceType::Bishop, Color::White);
constexpr Piece whiteRook = Piece(PieceType::Rook, Color::White);
constexpr Piece whiteQueen = Piece(PieceType::Queen, Color::White);
constexpr Piece whiteKing = Piece(PieceType::King, Color::White);

constexpr Piece blackPawn = Piece(PieceType::Pawn, Color::Black);
constexpr Piece blackKnight = Piece(PieceType::Knight, Color::Black);
constexpr Piece blackBishop = Piece(PieceType::Bishop, Color::Black);
constexpr Piece blackRook = Piece(PieceType::Rook, Color::Black);
constexpr Piece blackQueen = Piece(PieceType::Queen, Color::Black);
constexpr Piece blackKing = Piece(PieceType::King, Color::Black);

template <>
struct EnumTraits<Piece>
{
    using IdType = int;
    using EnumType = Piece;

    static constexpr int cardinality = 13;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        whitePawn,
        blackPawn,
        whiteKnight,
        blackKnight,
        whiteBishop,
        blackBishop,
        whiteRook,
        blackRook,
        whiteQueen,
        blackQueen,
        whiteKing,
        blackKing,
        Piece::none()
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(int id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality);

        return Piece::fromId(id);
    }

    [[nodiscard]] static constexpr std::string_view toString(EnumType p) noexcept
    {
        return std::string_view("PpNnBbRrQqKk " + ordinal(p), 1);
    }

    [[nodiscard]] static constexpr char toChar(EnumType p) noexcept
    {
        return "PpNnBbRrQqKk "[ordinal(p)];
    }

    [[nodiscard]] static constexpr std::optional<Piece> fromChar(char c) noexcept
    {
        auto it = std::string_view("PpNnBbRrQqKk ").find(c);
        if (it == std::string::npos) return {};
        else return Piece::fromId(static_cast<int>(it));
    }

    [[nodiscard]] static constexpr std::optional<Piece> fromString(std::string_view sv) noexcept
    {
        if (sv.size() != 1) return {};

        return fromChar(sv[0]);
    }
};

template <typename TagT>
struct Coord
{
    constexpr Coord() noexcept :
        m_i(0)
    {
    }

    constexpr explicit Coord(int i) noexcept :
        m_i(i)
    {
    }

    [[nodiscard]] constexpr explicit operator int() const
    {
        return static_cast<int>(m_i);
    }

    constexpr friend Coord& operator++(Coord& c)
    {
        ++c.m_i;
        return c;
    }

    constexpr friend Coord& operator--(Coord& c)
    {
        --c.m_i;
        return c;
    }

    constexpr friend Coord& operator+=(Coord& c, int d)
    {
        c.m_i += d;
        return c;
    }

    constexpr friend Coord& operator-=(Coord& c, int d)
    {
        c.m_i -= d;
        return c;
    }

    constexpr friend Coord operator+(const Coord& c, int d)
    {
        Coord cpy(c);
        cpy += d;
        return cpy;
    }

    constexpr friend Coord operator-(const Coord& c, int d)
    {
        Coord cpy(c);
        cpy -= d;
        return cpy;
    }

    constexpr friend int operator-(const Coord& c1, const Coord& c2)
    {
        return c1.m_i - c2.m_i;
    }

    [[nodiscard]] constexpr friend bool operator==(const Coord& c1, const Coord& c2) noexcept
    {
        return c1.m_i == c2.m_i;
    }

    [[nodiscard]] constexpr friend bool operator!=(const Coord& c1, const Coord& c2) noexcept
    {
        return c1.m_i != c2.m_i;
    }

    [[nodiscard]] constexpr friend bool operator<(const Coord& c1, const Coord& c2) noexcept
    {
        return c1.m_i < c2.m_i;
    }

    [[nodiscard]] constexpr friend bool operator<=(const Coord& c1, const Coord& c2) noexcept
    {
        return c1.m_i <= c2.m_i;
    }

    [[nodiscard]] constexpr friend bool operator>(const Coord& c1, const Coord& c2) noexcept
    {
        return c1.m_i > c2.m_i;
    }

    [[nodiscard]] constexpr friend bool operator>=(const Coord& c1, const Coord& c2) noexcept
    {
        return c1.m_i >= c2.m_i;
    }

private:
    std::int8_t m_i;
};

struct FileTag;
struct RankTag;
using File = Coord<FileTag>;
using Rank = Coord<RankTag>;

constexpr File fileA = File(0);
constexpr File fileB = File(1);
constexpr File fileC = File(2);
constexpr File fileD = File(3);
constexpr File fileE = File(4);
constexpr File fileF = File(5);
constexpr File fileG = File(6);
constexpr File fileH = File(7);

constexpr Rank rank1 = Rank(0);
constexpr Rank rank2 = Rank(1);
constexpr Rank rank3 = Rank(2);
constexpr Rank rank4 = Rank(3);
constexpr Rank rank5 = Rank(4);
constexpr Rank rank6 = Rank(5);
constexpr Rank rank7 = Rank(6);
constexpr Rank rank8 = Rank(7);

template <>
struct EnumTraits<File>
{
    using IdType = int;
    using EnumType = File;

    static constexpr int cardinality = 8;
    static constexpr bool isNaturalIndex = true;

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality);

        return static_cast<EnumType>(id);
    }

    [[nodiscard]] static constexpr std::string_view toString(EnumType c) noexcept
    {
        ASSERT(ordinal(c) >= 0 && ordinal(c) < 8);

        return std::string_view("abcdefgh" + ordinal(c), 1);
    }

    [[nodiscard]] static constexpr std::optional<File> fromChar(char c) noexcept
    {
        if (c < 'a' || c > 'h') return {};
        return static_cast<File>(c - 'a');
    }

    [[nodiscard]] static constexpr std::optional<File> fromString(std::string_view sv) noexcept
    {
        if (sv.size() != 1) return {};

        return fromChar(sv[0]);
    }
};

template <>
struct EnumTraits<Rank>
{
    using IdType = int;
    using EnumType = Rank;

    static constexpr int cardinality = 8;
    static constexpr bool isNaturalIndex = true;

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality);

        return static_cast<EnumType>(id);
    }

    [[nodiscard]] static constexpr std::string_view toString(EnumType c) noexcept
    {
        ASSERT(ordinal(c) >= 0 && ordinal(c) < 8);

        return std::string_view("12345678" + ordinal(c), 1);
    }

    [[nodiscard]] static constexpr std::optional<Rank> fromChar(char c) noexcept
    {
        if (c < '1' || c > '8') return {};
        return static_cast<Rank>(c - '1');
    }

    [[nodiscard]] static constexpr std::optional<Rank> fromString(std::string_view sv) noexcept
    {
        if (sv.size() != 1) return {};

        return fromChar(sv[0]);
    }
};

// files east
// ranks north
struct FlatSquareOffset
{
    std::int8_t value;

    constexpr FlatSquareOffset() noexcept :
        value(0)
    {
    }

    constexpr FlatSquareOffset(int files, int ranks) noexcept :
        value(files + ranks * cardinality<File>())
    {
        ASSERT(files + ranks * cardinality<File>() >= std::numeric_limits<std::int8_t>::min());
        ASSERT(files + ranks * cardinality<File>() <= std::numeric_limits<std::int8_t>::max());
    }
};

struct Offset
{
    std::int8_t files;
    std::int8_t ranks;

    [[nodiscard]] constexpr FlatSquareOffset flat() const
    {
        return { files, ranks };
    }
};

struct SquareCoords
{
    File file;
    Rank rank;

    constexpr SquareCoords() noexcept :
        file{},
        rank{}
    {
    }

    constexpr SquareCoords(File f, Rank r) noexcept :
        file(f),
        rank(r)
    {
    }

    constexpr friend SquareCoords& operator+=(SquareCoords& c, Offset offset)
    {
        c.file += offset.files;
        c.rank += offset.ranks;
        return c;
    }

    [[nodiscard]] constexpr friend SquareCoords operator+(const SquareCoords& c, Offset offset)
    {
        SquareCoords cpy(c);
        cpy.file += offset.files;
        cpy.rank += offset.ranks;
        return cpy;
    }

    [[nodiscard]] constexpr bool isOk() const
    {
        return file >= fileA && file <= fileH && rank >= rank1 && rank <= rank8;
    }
};

struct Square
{
private:
    static constexpr std::int8_t m_noneId = cardinality<Rank>() * cardinality<File>();

    static constexpr std::uint8_t fileMask = 0b111;
    static constexpr std::uint8_t rankMask = 0b111000;
    static constexpr std::uint8_t rankShift = 3;

public:
    [[nodiscard]] static constexpr Square none()
    {
        return Square(m_noneId);
    }

    constexpr Square() noexcept :
        m_id(0)
    {
    }

    constexpr explicit Square(int idx) noexcept :
        m_id(idx)
    {
        ASSERT(isOk() || m_id == m_noneId);
    }

    constexpr Square(File file, Rank rank) noexcept :
        m_id(ordinal(file) + ordinal(rank) * cardinality<File>())
    {
        ASSERT(isOk());
    }

    constexpr explicit Square(SquareCoords coords) noexcept :
        Square(coords.file, coords.rank)
    {
    }

    [[nodiscard]] constexpr friend bool operator<(Square lhs, Square rhs) noexcept
    {
        return lhs.m_id < rhs.m_id;
    }

    [[nodiscard]] constexpr friend bool operator>(Square lhs, Square rhs) noexcept
    {
        return lhs.m_id > rhs.m_id;
    }

    [[nodiscard]] constexpr friend bool operator<=(Square lhs, Square rhs) noexcept
    {
        return lhs.m_id <= rhs.m_id;
    }

    [[nodiscard]] constexpr friend bool operator>=(Square lhs, Square rhs) noexcept
    {
        return lhs.m_id >= rhs.m_id;
    }

    [[nodiscard]] constexpr friend bool operator==(Square lhs, Square rhs) noexcept
    {
        return lhs.m_id == rhs.m_id;
    }

    [[nodiscard]] constexpr friend bool operator!=(Square lhs, Square rhs) noexcept
    {
        return !(lhs == rhs);
    }

    constexpr friend Square& operator++(Square& sq)
    {
        ++sq.m_id;
        return sq;
    }

    constexpr friend Square& operator--(Square& sq)
    {
        --sq.m_id;
        return sq;
    }

    [[nodiscard]] constexpr friend Square operator+(Square sq, FlatSquareOffset offset)
    {
        Square sqCpy = sq;
        sqCpy += offset;
        return sqCpy;
    }

    constexpr friend Square& operator+=(Square& sq, FlatSquareOffset offset)
    {
        ASSERT(sq.m_id + offset.value >= 0 && sq.m_id + offset.value < Square::m_noneId);
        sq.m_id += offset.value;
        return sq;
    }

    [[nodiscard]] constexpr friend Square operator+(Square sq, Offset offset)
    {
        ASSERT(sq.file() + offset.files >= fileA);
        ASSERT(sq.file() + offset.files <= fileH);
        ASSERT(sq.rank() + offset.ranks >= rank1);
        ASSERT(sq.rank() + offset.ranks <= rank8);
        return operator+(sq, offset.flat());
    }

    constexpr friend Square& operator+=(Square& sq, Offset offset)
    {
        return operator+=(sq, offset.flat());
    }

    [[nodiscard]] constexpr explicit operator int() const
    {
        return m_id;
    }

    [[nodiscard]] constexpr File file() const
    {
        ASSERT(isOk());
        return File(static_cast<unsigned>(m_id) & fileMask);
    }

    [[nodiscard]] constexpr Rank rank() const
    {
        ASSERT(isOk());
        return Rank(static_cast<unsigned>(m_id) >> rankShift);
    }

    [[nodiscard]] constexpr SquareCoords coords() const
    {
        return { file(), rank() };
    }

    [[nodiscard]] constexpr Color color() const
    {
        ASSERT(isOk());
        return !fromOrdinal<Color>(ordinal(rank()) + ordinal(file()) & 1);
    }

    constexpr void flipVertically()
    {
        m_id ^= 0b111;
    }

    constexpr void flipHorizontally()
    {
        m_id ^= 0b111000;
    }

    [[nodiscard]] constexpr bool isOk() const
    {
        return m_id >= 0 && m_id < m_noneId;
    }

private:
    std::int8_t m_id;
};

constexpr Square a1(fileA, rank1);
constexpr Square a2(fileA, rank2);
constexpr Square a3(fileA, rank3);
constexpr Square a4(fileA, rank4);
constexpr Square a5(fileA, rank5);
constexpr Square a6(fileA, rank6);
constexpr Square a7(fileA, rank7);
constexpr Square a8(fileA, rank8);

constexpr Square b1(fileB, rank1);
constexpr Square b2(fileB, rank2);
constexpr Square b3(fileB, rank3);
constexpr Square b4(fileB, rank4);
constexpr Square b5(fileB, rank5);
constexpr Square b6(fileB, rank6);
constexpr Square b7(fileB, rank7);
constexpr Square b8(fileB, rank8);

constexpr Square c1(fileC, rank1);
constexpr Square c2(fileC, rank2);
constexpr Square c3(fileC, rank3);
constexpr Square c4(fileC, rank4);
constexpr Square c5(fileC, rank5);
constexpr Square c6(fileC, rank6);
constexpr Square c7(fileC, rank7);
constexpr Square c8(fileC, rank8);

constexpr Square d1(fileD, rank1);
constexpr Square d2(fileD, rank2);
constexpr Square d3(fileD, rank3);
constexpr Square d4(fileD, rank4);
constexpr Square d5(fileD, rank5);
constexpr Square d6(fileD, rank6);
constexpr Square d7(fileD, rank7);
constexpr Square d8(fileD, rank8);

constexpr Square e1(fileE, rank1);
constexpr Square e2(fileE, rank2);
constexpr Square e3(fileE, rank3);
constexpr Square e4(fileE, rank4);
constexpr Square e5(fileE, rank5);
constexpr Square e6(fileE, rank6);
constexpr Square e7(fileE, rank7);
constexpr Square e8(fileE, rank8);

constexpr Square f1(fileF, rank1);
constexpr Square f2(fileF, rank2);
constexpr Square f3(fileF, rank3);
constexpr Square f4(fileF, rank4);
constexpr Square f5(fileF, rank5);
constexpr Square f6(fileF, rank6);
constexpr Square f7(fileF, rank7);
constexpr Square f8(fileF, rank8);

constexpr Square g1(fileG, rank1);
constexpr Square g2(fileG, rank2);
constexpr Square g3(fileG, rank3);
constexpr Square g4(fileG, rank4);
constexpr Square g5(fileG, rank5);
constexpr Square g6(fileG, rank6);
constexpr Square g7(fileG, rank7);
constexpr Square g8(fileG, rank8);

constexpr Square h1(fileH, rank1);
constexpr Square h2(fileH, rank2);
constexpr Square h3(fileH, rank3);
constexpr Square h4(fileH, rank4);
constexpr Square h5(fileH, rank5);
constexpr Square h6(fileH, rank6);
constexpr Square h7(fileH, rank7);
constexpr Square h8(fileH, rank8);

static_assert(e1.color() == Color::Black);
static_assert(e8.color() == Color::White);

template <>
struct EnumTraits<Square>
{
    using IdType = int;
    using EnumType = Square;

    static constexpr int cardinality = ::cardinality<Rank>() * ::cardinality<File>();
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        a1, b1, c1, d1, e1, f1, g1, h1,
        a2, b2, c2, d2, e2, f2, g2, h2,
        a3, b3, c3, d3, e3, f3, g3, h3,
        a4, b4, c4, d4, e4, f4, g4, h4,
        a5, b5, c5, d5, e5, f5, g5, h5,
        a6, b6, c6, d6, e6, f6, g6, h6,
        a7, b7, c7, d7, e7, f7, g7, h7,
        a8, b8, c8, d8, e8, f8, g8, h8
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality + 1);

        return static_cast<EnumType>(id);
    }

    [[nodiscard]] static constexpr std::string_view toString(Square sq)
    {
        ASSERT(sq.isOk());

        return
            std::string_view(
                "a1b1c1d1e1f1g1h1"
                "a2b2c2d2e2f2g2h2"
                "a3b3c3d3e3f3g3h3"
                "a4b4c4d4e4f4g4h4"
                "a5b5c5d5e5f5g5h5"
                "a6b6c6d6e6f6g6h6"
                "a7b7c7d7e7f7g7h7"
                "a8b8c8d8e8f8g8h8"
                + (ordinal(sq) * 2),
                2
            );
    }

    [[nodiscard]] static constexpr std::optional<Square> fromString(std::string_view sv) noexcept
    {
        if (sv.size() != 2) return {};

        const char f = sv[0];
        const char r = sv[1];
        if (f < 'a' || f > 'h') return {};
        if (r < '1' || r > '8') return {};

        return Square(static_cast<File>(f - 'a'), static_cast<Rank>(r - '1'));
    }
};

static_assert(toString(d1) == std::string_view("d1"));
static_assert(values<Square>()[29] == f4);

enum struct MoveType : std::uint8_t
{
    Normal,
    Promotion,
    Castle,
    EnPassant
};

template <>
struct EnumTraits<MoveType>
{
    using IdType = int;
    using EnumType = MoveType;

    static constexpr int cardinality = 4;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        MoveType::Normal,
        MoveType::Promotion,
        MoveType::Castle,
        MoveType::EnPassant
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality);

        return static_cast<EnumType>(id);
    }
};

enum struct CastleType : std::uint8_t
{
    Short,
    Long
};

template <>
struct EnumTraits<CastleType>
{
    using IdType = int;
    using EnumType = CastleType;

    static constexpr int cardinality = 2;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        CastleType::Short,
        CastleType::Long
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        ASSERT(id >= 0 && id < cardinality);

        return static_cast<EnumType>(id);
    }
};

struct CompressedMove;

// castling is encoded as a king capturing rook
// ep is encoded as a normal pawn capture (move.to is empty on the board)
struct Move
{
    Square from;
    Square to;
    MoveType type = MoveType::Normal;
    Piece promotedPiece = Piece::none();

    [[nodiscard]] constexpr friend bool operator==(const Move& lhs, const Move& rhs) noexcept
    {
        return lhs.from == rhs.from
            && lhs.to == rhs.to
            && lhs.type == rhs.type
            && lhs.promotedPiece == rhs.promotedPiece;
    }

    [[nodiscard]] constexpr friend bool operator!=(const Move& lhs, const Move& rhs) noexcept
    {
        return !(lhs == rhs);
    }

    [[nodiscard]] constexpr CompressedMove compress() const noexcept;

    [[nodiscard]] constexpr static Move null()
    {
        return Move{ Square::none(), Square::none() };
    }

    [[nodiscard]] constexpr static Move castle(CastleType ct, Color c);

    [[nodiscard]] constexpr static Move normal(Square from, Square to)
    {
        return Move{ from, to, MoveType::Normal, Piece::none() };
    }

    [[nodiscard]] constexpr static Move enPassant(Square from, Square to)
    {
        return Move{ from, to, MoveType::EnPassant, Piece::none() };
    }

    [[nodiscard]] constexpr static Move promotion(Square from, Square to, Piece piece)
    {
        return Move{ from, to, MoveType::Promotion, piece };
    }
};

static_assert(sizeof(Move) == 4);

struct CompressedMove
{
private:
    // from most significant bits
    // 2 bits for move type
    // 6 bits for from square
    // 6 bits for to square
    // 2 bits for promoted piece type
    //    0 if not a promotion
    static constexpr std::uint16_t squareMask = 0b111111u;
    static constexpr std::uint16_t promotedPieceTypeMask = 0b11u;
    static constexpr std::uint16_t moveTypeMask = 0b11u;

public:
    [[nodiscard]] constexpr static CompressedMove readFromBigEndian(const unsigned char* data)
    {
        CompressedMove move{};
        move.m_packed = (data[0] << 8) | data[1];
        return move;
    }

    constexpr CompressedMove() noexcept :
        m_packed(0)
    {
    }

    // move must be either valid or a null move
    constexpr CompressedMove(Move move) noexcept :
        m_packed(0)
    {
        // else null move
        if (move.from != move.to)
        {
            ASSERT(move.from != Square::none());
            ASSERT(move.to != Square::none());

            m_packed =
                (static_cast<std::uint16_t>(ordinal(move.type)) << (16 - 2))
                | (static_cast<std::uint16_t>(ordinal(move.from)) << (16 - 2 - 6))
                | (static_cast<std::uint16_t>(ordinal(move.to)) << (16 - 2 - 6 - 6));

            if (move.type == MoveType::Promotion)
            {
                ASSERT(move.promotedPiece != Piece::none());

                m_packed |= ordinal(move.promotedPiece.type()) - ordinal(PieceType::Knight);
            }
            else
            {
                ASSERT(move.promotedPiece == Piece::none());
            }
        }
    }

    void writeToBigEndian(unsigned char* data) const
    {
        *data++ = m_packed >> 8;
        *data++ = m_packed & 0xFF;
    }

    [[nodiscard]] constexpr std::uint16_t packed() const
    {
        return m_packed;
    }

    [[nodiscard]] constexpr MoveType type() const
    {
        return fromOrdinal<MoveType>(m_packed >> (16 - 2));
    }

    [[nodiscard]] constexpr Square from() const
    {
        return fromOrdinal<Square>((m_packed >> (16 - 2 - 6)) & squareMask);
    }

    [[nodiscard]] constexpr Square to() const
    {
        return fromOrdinal<Square>((m_packed >> (16 - 2 - 6 - 6)) & squareMask);
    }

    [[nodiscard]] constexpr Piece promotedPiece() const
    {
        if (type() == MoveType::Promotion)
        {
            const Color color =
                (to().rank() == rank1)
                ? Color::Black
                : Color::White;

            const PieceType pt = fromOrdinal<PieceType>((m_packed & promotedPieceTypeMask) + ordinal(PieceType::Knight));
            return color | pt;
        }
        else
        {
            return Piece::none();
        }
    }

    [[nodiscard]] constexpr Move decompress() const noexcept
    {
        if (m_packed == 0)
        {
            return Move::null();
        }
        else
        {
            const MoveType type = fromOrdinal<MoveType>(m_packed >> (16 - 2));
            const Square from = fromOrdinal<Square>((m_packed >> (16 - 2 - 6)) & squareMask);
            const Square to = fromOrdinal<Square>((m_packed >> (16 - 2 - 6 - 6)) & squareMask);
            const Piece promotedPiece = [&]() {
                if (type == MoveType::Promotion)
                {
                    const Color color =
                        (to.rank() == rank1)
                        ? Color::Black
                        : Color::White;

                    const PieceType pt = fromOrdinal<PieceType>((m_packed & promotedPieceTypeMask) + ordinal(PieceType::Knight));
                    return color | pt;
                }
                else
                {
                    return Piece::none();
                }
            }();

            return Move{ from, to, type, promotedPiece };
        }
    }

private:
    std::uint16_t m_packed;
};

static_assert(sizeof(CompressedMove) == 2);

[[nodiscard]] constexpr CompressedMove Move::compress() const noexcept
{
    return CompressedMove(*this);
}

namespace detail::castle
{
    constexpr EnumArray2<CastleType, Color, Move> moves = { {
        {{ { e1, h1, MoveType::Castle }, { e8, h8, MoveType::Castle } }},
        {{ { e1, a1, MoveType::Castle }, { e8, a8, MoveType::Castle } }}
    } };
}

[[nodiscard]] constexpr Move Move::castle(CastleType ct, Color c)
{
    return detail::castle::moves[ct][c];
}

static_assert(a4 + Offset{ 0, 1 } == a5);
static_assert(a4 + Offset{ 0, 2 } == a6);
static_assert(a4 + Offset{ 0, -2 } == a2);
static_assert(a4 + Offset{ 0, -1 } == a3);

static_assert(e4 + Offset{ 1, 0 } == f4);
static_assert(e4 + Offset{ 2, 0 } == g4);
static_assert(e4 + Offset{ -1, 0 } == d4);
static_assert(e4 + Offset{ -2, 0 } == c4);

enum struct CastlingRights : std::uint8_t
{
    None = 0x0,
    WhiteKingSide = 0x1,
    WhiteQueenSide = 0x2,
    BlackKingSide = 0x4,
    BlackQueenSide = 0x8,
    White = WhiteKingSide | WhiteQueenSide,
    Black = BlackKingSide | BlackQueenSide,
    All = WhiteKingSide | WhiteQueenSide | BlackKingSide | BlackQueenSide
};

[[nodiscard]] constexpr CastlingRights operator|(CastlingRights lhs, CastlingRights rhs)
{
    return static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
}

[[nodiscard]] constexpr CastlingRights operator&(CastlingRights lhs, CastlingRights rhs)
{
    return static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
}

[[nodiscard]] constexpr CastlingRights operator~(CastlingRights lhs)
{
    return static_cast<CastlingRights>(~static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(CastlingRights::All));
}

constexpr CastlingRights& operator|=(CastlingRights& lhs, CastlingRights rhs)
{
    lhs = static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
    return lhs;
}

constexpr CastlingRights& operator&=(CastlingRights& lhs, CastlingRights rhs)
{
    lhs = static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
    return lhs;
}

constexpr CastlingRights moveToCastlingType(Move move)
{
    if (move.to == h1) return CastlingRights::WhiteKingSide;
    if (move.to == a1) return CastlingRights::WhiteQueenSide;
    if (move.to == h8) return CastlingRights::WhiteKingSide;
    if (move.to == a8) return CastlingRights::WhiteQueenSide;
    return CastlingRights::None;
}

// checks whether lhs contains rhs
[[nodiscard]] constexpr bool contains(CastlingRights lhs, CastlingRights rhs)
{
    return (lhs & rhs) == rhs;
}

template <>
struct EnumTraits<CastlingRights>
{
    using IdType = int;
    using EnumType = CastlingRights;

    static constexpr int cardinality = 4;
    static constexpr bool isNaturalIndex = false;

    static constexpr std::array<EnumType, cardinality> values{
        CastlingRights::WhiteKingSide,
        CastlingRights::WhiteQueenSide,
        CastlingRights::BlackKingSide,
        CastlingRights::BlackQueenSide
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        return static_cast<EnumType>(id);
    }
};

struct CompressedReverseMove;

struct ReverseMove
{
    Move move;
    Piece capturedPiece;
    Square oldEpSquare;
    CastlingRights oldCastlingRights;

    // We need a well defined case for the starting position.
    constexpr ReverseMove() :
        move(Move::null()),
        capturedPiece(Piece::none()),
        oldEpSquare(Square::none()),
        oldCastlingRights(CastlingRights::All)
    {
    }

    constexpr ReverseMove(const Move& move, Piece capturedPiece, Square oldEpSquare, CastlingRights oldCastlingRights) :
        move(move),
        capturedPiece(capturedPiece),
        oldEpSquare(oldEpSquare),
        oldCastlingRights(oldCastlingRights)
    {
    }

    constexpr bool isNull() const
    {
        return move.from == move.to;
    }

    [[nodiscard]] constexpr CompressedReverseMove compress() const noexcept;
};

static_assert(sizeof(ReverseMove) == 7);

struct CompressedReverseMove
{
private:
    // we use 7 bits because it can be Square::none()
    static constexpr std::uint32_t squareMask = 0b1111111u;
    static constexpr std::uint32_t pieceMask = 0b1111u;
    static constexpr std::uint32_t castlingRightsMask = 0b1111;
public:

    constexpr CompressedReverseMove() noexcept :
        m_move{},
        m_oldState{}
    {
    }

    constexpr CompressedReverseMove(const ReverseMove& rm) noexcept :
        m_move(rm.move.compress()),
        m_oldState{ static_cast<uint16_t>(
            ((ordinal(rm.capturedPiece) & pieceMask) << 11)
            | ((ordinal(rm.oldCastlingRights) & castlingRightsMask) << 7)
            | (ordinal(rm.oldEpSquare) & squareMask)
            )
        }
    {
    }

    [[nodiscard]] constexpr Move move() const
    {
        return m_move.decompress();
    }

    [[nodiscard]] const CompressedMove& compressedMove() const
    {
        return m_move;
    }

    [[nodiscard]] constexpr Piece capturedPiece() const
    {
        return fromOrdinal<Piece>(m_oldState >> 11);
    }

    [[nodiscard]] constexpr CastlingRights oldCastlingRights() const
    {
        return fromOrdinal<CastlingRights>((m_oldState >> 7) & castlingRightsMask);
    }

    [[nodiscard]] constexpr Square oldEpSquare() const
    {
        return fromOrdinal<Square>(m_oldState & squareMask);
    }

    [[nodiscard]] constexpr ReverseMove decompress() const noexcept
    {
        const Piece capturedPiece = fromOrdinal<Piece>(m_oldState >> 11);
        const CastlingRights castlingRights = fromOrdinal<CastlingRights>((m_oldState >> 7) & castlingRightsMask);
        // We could pack the ep square more, but don't have to, because
        // can't save another byte anyway.
        const Square epSquare = fromOrdinal<Square>(m_oldState & squareMask);

        return ReverseMove(m_move.decompress(), capturedPiece, epSquare, castlingRights);
    }

private:
    CompressedMove m_move;
    std::uint16_t m_oldState;
};

static_assert(sizeof(CompressedReverseMove) == 4);

[[nodiscard]] constexpr CompressedReverseMove ReverseMove::compress() const noexcept
{
    return CompressedReverseMove(*this);
}

// This can be regarded as a perfect hash. Going back is hard.
struct PackedReverseMove
{
    static constexpr std::uint32_t mask = 0x7FFFFFFu;
    static constexpr std::size_t numBits = 27;

private:
    static constexpr std::uint32_t squareMask = 0b111111u;
    static constexpr std::uint32_t pieceMask = 0b1111u;
    static constexpr std::uint32_t pieceTypeMask = 0b111u;
    static constexpr std::uint32_t castlingRightsMask = 0b1111;
    static constexpr std::uint32_t fileMask = 0b111;

public:
    constexpr PackedReverseMove(const std::uint32_t packed) :
        m_packed(packed)
    {
        
    }

    constexpr PackedReverseMove(const ReverseMove& reverseMove) :
        m_packed(
            0u
            // The only move when square is none() is null move and
            // then both squares are none(). No other move is like that
            // so we don't lose any information by storing only
            // the 6 bits of each square.
            | ((ordinal(reverseMove.move.from) & squareMask) << 21)
            | ((ordinal(reverseMove.move.to) & squareMask) << 15)
            // Other masks are just for code clarity, they should
            // never change the values.
            | ((ordinal(reverseMove.capturedPiece) & pieceMask) << 11)
            | ((ordinal(reverseMove.oldCastlingRights) & castlingRightsMask) << 7)
            | ((ordinal(reverseMove.move.promotedPiece.type()) & pieceTypeMask) << 4)
            | (((reverseMove.oldEpSquare != Square::none()) & 1) << 3)
            // We probably could omit the squareMask here but for clarity it's left.
            | (ordinal(Square(ordinal(reverseMove.oldEpSquare) & squareMask).file()) & fileMask)
        )
    {
    }

    constexpr std::uint32_t packed() const
    {
        return m_packed;
    }

    constexpr ReverseMove unpack(Color sideThatMoved) const
    {
        ReverseMove rmove{};

        rmove.move.from = fromOrdinal<Square>((m_packed >> 21) & squareMask);
        rmove.move.to = fromOrdinal<Square>((m_packed >> 15) & squareMask);
        rmove.capturedPiece = fromOrdinal<Piece>((m_packed >> 11) & pieceMask);
        rmove.oldCastlingRights = fromOrdinal<CastlingRights>((m_packed >> 7) & castlingRightsMask);
        const PieceType promotedPieceType = fromOrdinal<PieceType>((m_packed >> 4) & pieceTypeMask);
        if (promotedPieceType != PieceType::None)
        {
            rmove.move.promotedPiece = Piece(promotedPieceType, sideThatMoved);
            rmove.move.type = MoveType::Promotion;
        }
        const bool hasEpSquare = static_cast<bool>((m_packed >> 3) & 1);
        if (hasEpSquare)
        {
            // ep square is always where the opponent moved
            const Rank rank =
                sideThatMoved == Color::White
                ? rank6
                : rank3;
            const File file = fromOrdinal<File>(m_packed & fileMask);
            rmove.oldEpSquare = Square(file, rank);
            if (rmove.oldEpSquare == rmove.move.to)
            {
                rmove.move.type = MoveType::EnPassant;
            }
        }
        else
        {
            rmove.oldEpSquare = Square::none();
        }

        if (rmove.move.type == MoveType::Normal && rmove.oldCastlingRights != CastlingRights::None)
        {
            // If castling was possible then we know it was the king that moved from e1/e8.
            if (rmove.move.from == e1)
            {
                if (rmove.move.to == h1 || rmove.move.to == a1)
                {
                    rmove.move.type = MoveType::Castle;
                }
            }
            else if (rmove.move.from == e8)
            {
                if (rmove.move.to == h8 || rmove.move.to == a8)
                {
                    rmove.move.type = MoveType::Castle;
                }
            }
        }

        return rmove;
    }

private:
    // Uses only 27 lowest bits.
    // Bit meaning from highest to lowest.
    // - 6 bits from
    // - 6 bits to
    // - 4 bits for the captured piece
    // - 4 bits for prev castling rights
    // - 3 bits promoted piece type
    // - 1 bit  to specify if the ep square was valid (false if none())
    // - 3 bits for prev ep square file
    std::uint32_t m_packed;
};

struct MoveCompareLess
{
    [[nodiscard]] bool operator()(const Move& lhs, const Move& rhs) const noexcept
    {
        if (ordinal(lhs.from) < ordinal(rhs.from)) return true;
        if (ordinal(lhs.from) > ordinal(rhs.from)) return false;

        if (ordinal(lhs.to) < ordinal(rhs.to)) return true;
        if (ordinal(lhs.to) > ordinal(rhs.to)) return false;

        if (ordinal(lhs.type) < ordinal(rhs.type)) return true;
        if (ordinal(lhs.type) > ordinal(rhs.type)) return false;

        if (ordinal(lhs.promotedPiece) < ordinal(rhs.promotedPiece)) return true;

        return false;
    }
};

struct ReverseMoveCompareLess
{
    [[nodiscard]] bool operator()(const ReverseMove& lhs, const ReverseMove& rhs) const noexcept
    {
        if (MoveCompareLess{}(lhs.move, rhs.move)) return true;
        if (MoveCompareLess{}(rhs.move, lhs.move)) return false;

        if (ordinal(lhs.capturedPiece) < ordinal(rhs.capturedPiece)) return true;
        if (ordinal(lhs.capturedPiece) > ordinal(rhs.capturedPiece)) return false;

        if (static_cast<unsigned>(lhs.oldCastlingRights) < static_cast<unsigned>(rhs.oldCastlingRights)) return true;
        if (static_cast<unsigned>(lhs.oldCastlingRights) > static_cast<unsigned>(rhs.oldCastlingRights)) return false;

        if (ordinal(lhs.oldEpSquare) < ordinal(rhs.oldEpSquare)) return true;
        if (ordinal(lhs.oldEpSquare) > ordinal(rhs.oldEpSquare)) return false;

        return false;
    }
};
