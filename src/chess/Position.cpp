#include "Position.h"

#include "detail/ParserBits.h"

#include "Bitboard.h"
#include "Chess.h"

#include "data_structure/EnumMap.h"

#include "util/Assert.h"

#include <iterator>
#include <optional>
#include <string>

#include "xxhash/xxhash_cpp.h"

[[nodiscard]] bool Board::createsDiscoveredAttackOnOwnKing(Move move, Color color) const
{
    // checks whether by doing a move we uncover our king to a check
    // doesn't verify castlings as it is supposed to only cover undiscovered checks

    ASSERT(move.from.isOk() && move.to.isOk());
    ASSERT(move.type != MoveType::Castle);

    const Square ksq = kingSquare(color);

    ASSERT(ksq != move.from);

    if (move.type == MoveType::Castle)
    {
        return false;
    }

    Bitboard occupied = (piecesBB() ^ move.from) | move.to;
    Bitboard captured = Bitboard::none();
    Bitboard removed = Bitboard::square(move.from);

    if (move.type == MoveType::EnPassant)
    {
        const Square capturedPieceSq(move.to.file(), move.from.rank());
        occupied ^= capturedPieceSq;
        removed |= capturedPieceSq;
        // We don't update captured becuase it only affects pawns - we don't care.
    }
    else if (m_pieces[move.to] != Piece::none())
    {
        // A capture happened.
        // We have to exclude the captured piece.
        captured |= move.to;
    }

    const Bitboard allSliderPseudoAttacks = bb::pseudoAttacks<PieceType::Queen>(ksq);
    if (!(allSliderPseudoAttacks & removed).any())
    {
        // if the square is not aligned with the king we don't have to check anything
        return false;
    }

    const Bitboard bishops = piecesBB(Piece(PieceType::Bishop, !color)) & ~captured;
    const Bitboard rooks = piecesBB(Piece(PieceType::Rook, !color)) & ~captured;
    const Bitboard queens = piecesBB(Piece(PieceType::Queen, !color)) & ~captured;
    if (!(allSliderPseudoAttacks & (bishops | rooks | queens)).any())
    {
        return false;
    }

    return bb::isAttackedBySlider(
        ksq,
        bishops,
        rooks,
        queens,
        occupied
    );
}

[[nodiscard]] bool Board::isSquareAttacked(Square sq, Color attackerColor, Bitboard occupied, Bitboard captured) const
{
    ASSERT(sq.isOk());

    const Bitboard bishops = piecesBB(Piece(PieceType::Bishop, attackerColor)) & ~captured;
    const Bitboard rooks = piecesBB(Piece(PieceType::Rook, attackerColor)) & ~captured;
    const Bitboard queens = piecesBB(Piece(PieceType::Queen, attackerColor)) & ~captured;
    if ((bb::pseudoAttacks<PieceType::Queen>(sq) & (bishops | rooks | queens)).any())
    {
        if (bb::isAttackedBySlider(
            sq,
            bishops,
            rooks,
            queens,
            occupied
        ))
        {
            return true;
        }
    }

    if (bb::pseudoAttacks<PieceType::King>(sq).isSet(kingSquare(attackerColor)))
    {
        return true;
    }

    if ((bb::pseudoAttacks<PieceType::Knight>(sq) & m_pieceBB[Piece(PieceType::Knight, attackerColor)] & ~captured).any())
    {
        return true;
    }

    // Check pawn attacks. Nothing else can attack the square at this point.
    const Bitboard pawns = m_pieceBB[Piece(PieceType::Pawn, attackerColor)] & ~captured;
    const Bitboard pawnAttacks = bb::pawnAttacks(pawns, attackerColor);

    return pawnAttacks.isSet(sq);
}

[[nodiscard]] bool Board::isSquareAttacked(Square sq, Color attackerColor) const
{
    return isSquareAttacked(sq, attackerColor, piecesBB(), Bitboard::none());
}

[[nodiscard]] bool Board::isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const
{
    // TODO: See whether this can be done better.
    Board cpy(*this);
    cpy.doMove(move);
    return cpy.isSquareAttacked(sq, attackerColor);
}

[[nodiscard]] bool Board::isKingAttackedAfterMove(Move move, Color kingColor) const
{
    // TODO: See whether this can be done better.
    Board cpy(*this);
    cpy.doMove(move);
    return cpy.isSquareAttacked(cpy.kingSquare(kingColor), !kingColor);
}

const Piece* Board::piecesRaw() const
{
    return m_pieces.data();
}

namespace detail::lookup
{
    static constexpr EnumMap<Piece, char> fenPiece = []() {
        EnumMap<Piece, char> fenPiece{};

        fenPiece[whitePawn] = 'P';
        fenPiece[blackPawn] = 'p';
        fenPiece[whiteKnight] = 'N';
        fenPiece[blackKnight] = 'n';
        fenPiece[whiteBishop] = 'B';
        fenPiece[blackBishop] = 'b';
        fenPiece[whiteRook] = 'R';
        fenPiece[blackRook] = 'r';
        fenPiece[whiteQueen] = 'Q';
        fenPiece[blackQueen] = 'q';
        fenPiece[whiteKing] = 'K';
        fenPiece[blackKing] = 'k';
        fenPiece[Piece::none()] = 'X';

        return fenPiece;
    }();
}

[[nodiscard]] std::string Board::fen() const
{
    std::string fen;
    fen.reserve(96); // longest fen is probably in range of around 88

    Rank rank = rank8;
    File file = fileA;
    std::uint8_t emptyCounter = 0;

    for (;;)
    {
        const Square sq(file, rank);
        const Piece piece = m_pieces[sq];

        if (piece == Piece::none())
        {
            ++emptyCounter;
        }
        else
        {
            if (emptyCounter != 0)
            {
                fen.push_back(static_cast<char>(emptyCounter) + '0');
                emptyCounter = 0;
            }

            fen.push_back(detail::lookup::fenPiece[piece]);
        }

        ++file;
        if (file > fileH)
        {
            file = fileA;
            --rank;
            if (rank < rank1)
            {
                break;
            }

            if (emptyCounter != 0)
            {
                fen.push_back(static_cast<char>(emptyCounter) + '0');
                emptyCounter = 0;
            }
            fen.push_back('/');
        }
    }

    return fen;
}

void Position::set(const char* fen)
{
    const char* s = BaseType::set(fen);

    s += 1;
    m_sideToMove = (*s == 'w') ? Color::White : Color::Black;

    s += 2;
    m_castlingRights = parser_bits::readCastlingRights(s);

    s += 1;
    m_epSquare = (*s == '-') ? Square::none() : parser_bits::parseSquare(s);

    nullifyEpSquareIfNotPossible();
}

// Returns false if the fen was not valid
// If the returned value was false the position
// is in unspecified state.
[[nodiscard]] bool Position::trySet(std::string_view fen)
{
    // Lazily splits by ' '. Returns empty string views if at the end.
    auto nextPart = [fen, start = std::size_t{ 0 }]() mutable {
        std::size_t end = fen.find(' ', start);
        if (end == std::string::npos)
        {
            std::string_view substr = fen.substr(start);
            start = fen.size();
            return substr;
        }
        else
        {
            std::string_view substr = fen.substr(start, end - start);
            start = end + 1; // to skip whitespace
            return substr;
        }
    };

    if (!BaseType::trySet(nextPart())) return false;

    {
        const auto side = nextPart();
        if (side == std::string_view("w")) m_sideToMove = Color::White;
        else if (side == std::string_view("b")) m_sideToMove = Color::Black;
        else return false;

        if (isSquareAttacked(kingSquare(!m_sideToMove), m_sideToMove)) return false;
    }

    {
        const auto castlingRights = nextPart();
        auto castlingRightsOpt = parser_bits::tryParseCastlingRights(castlingRights);
        if (!castlingRightsOpt.has_value())
        {
            return false;
        }
        else
        {
            m_castlingRights = *castlingRightsOpt;
        }
    }

    {
        const auto epSquare = nextPart();
        auto epSquareOpt = parser_bits::tryParseEpSquare(epSquare);
        if (!epSquareOpt.has_value())
        {
            return false;
        }
        else
        {
            m_epSquare = *epSquareOpt;
        }
    }

    nullifyEpSquareIfNotPossible();

    return true;
}

[[nodiscard]] Position Position::fromFen(const char* fen)
{
    Position pos{};
    pos.set(fen);
    return pos;
}

[[nodiscard]] std::optional<Position> Position::tryFromFen(std::string_view fen)
{
    Position pos{};
    if (pos.trySet(fen)) return pos;
    else return {};
}

[[nodiscard]] Position Position::startPosition()
{
    static const Position pos = fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");
    return pos;
}

[[nodiscard]] std::string Position::fen() const
{
    std::string fen = Board::fen();

    fen += ' ';
    fen += m_sideToMove == Color::White ? 'w' : 'b';

    fen += ' ';
    parser_bits::appendCastlingRightsToString(m_castlingRights, fen);

    fen += ' ';
    parser_bits::appendEpSquareToString(m_epSquare, fen);

    return fen;
}

void Position::setEpSquareUnchecked(Square sq)
{
    m_epSquare = sq;
}

void Position::setSideToMove(Color color)
{
    m_sideToMove = color;
}

void Position::addCastlingRights(CastlingRights rights)
{
    m_castlingRights |= rights;
}

void Position::setCastlingRights(CastlingRights rights)
{
    m_castlingRights = rights;
}

[[nodiscard]] bool Position::createsDiscoveredAttackOnOwnKing(Move move) const
{
    return BaseType::createsDiscoveredAttackOnOwnKing(move, m_sideToMove);
}

[[nodiscard]] bool Position::createsAttackOnOwnKing(Move move) const
{
    return BaseType::isKingAttackedAfterMove(move, m_sideToMove);
}

[[nodiscard]] bool Position::isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const
{
    return BaseType::isSquareAttackedAfterMove(sq, move, attackerColor);
}

[[nodiscard]] bool Position::isSquareAttacked(Square sq, Color attackerColor) const
{
    return BaseType::isSquareAttacked(sq, attackerColor);
}

namespace detail::lookup
{
    static constexpr EnumMap<Square, CastlingRights> preservedCastlingRights = []() {
        EnumMap<Square, CastlingRights> preservedCastlingRights{};
        for (CastlingRights& rights : preservedCastlingRights)
        {
            rights = ~CastlingRights::None;
        }

        preservedCastlingRights[e1] = ~CastlingRights::White;
        preservedCastlingRights[e8] = ~CastlingRights::Black;

        preservedCastlingRights[h1] = ~CastlingRights::WhiteKingSide;
        preservedCastlingRights[a1] = ~CastlingRights::WhiteQueenSide;
        preservedCastlingRights[h8] = ~CastlingRights::BlackKingSide;
        preservedCastlingRights[a8] = ~CastlingRights::BlackQueenSide;

        return preservedCastlingRights;
    }();
}

ReverseMove Position::doMove(const Move& move)
{
    ASSERT(move.from.isOk() && move.to.isOk());

    const PieceType movedPiece = pieceAt(move.from).type();
    const Square oldEpSquare = m_epSquare;
    const CastlingRights oldCastlingRights = m_castlingRights;
    m_castlingRights &= detail::lookup::preservedCastlingRights[move.from];

    m_epSquare = Square::none();
    if(movedPiece == PieceType::Pawn)
    {
        const int d = move.to.rank() - move.from.rank();
        if (d == -2 || d == 2)
        {
            const Square potentialEpSquare = Square(move.from.file(), move.from.rank() + d / 2);
            // Even though the move has not yet been made we can safely call
            // this function and get the right result because the position of the
            // pawn to be captured is not really relevant.
            if (isEpPossible(potentialEpSquare, !m_sideToMove))
            {
                m_epSquare = potentialEpSquare;
            }
        }
    }

    const Piece captured = BaseType::doMove(move);
    m_sideToMove = !m_sideToMove;
    return { move, captured, oldEpSquare, oldCastlingRights };
}

[[nodiscard]] bool Position::isLegal() const
{
    return piecesBB(Piece(PieceType::King, Color::White)).count() == 1
        && piecesBB(Piece(PieceType::King, Color::Black)).count() == 1
        && !isSquareAttacked(kingSquare(!m_sideToMove), m_sideToMove);
}

[[nodiscard]] bool Position::isCheck(Move move) const
{
    return BaseType::isSquareAttackedAfterMove(kingSquare(!m_sideToMove), move, m_sideToMove);
}

[[nodiscard]] Position Position::afterMove(Move move) const
{
    Position cpy(*this);
    auto pc = cpy.doMove(move);

    (void)pc;
    //ASSERT(cpy.beforeMove(move, pc) == *this); // this assert would result in infinite recursion

    return cpy;
}

[[nodiscard]] std::array<std::uint32_t, 4> Position::hash() const
{
    constexpr std::size_t epSquareFileBits = 4;
    constexpr std::size_t castlingRightsBits = 4;

    std::array<std::uint32_t, 4> arrh;
    auto h = xxhash::XXH3_128bits(piecesRaw(), 64);
    std::memcpy(arrh.data(), &h, sizeof(std::uint32_t) * 4);
    arrh[0] ^= ordinal(m_sideToMove);

    arrh[0] <<= epSquareFileBits;
    // 0xF is certainly not a file number
    arrh[0] ^= m_epSquare == Square::none() ? 0xF : ordinal(m_epSquare);

    arrh[0] <<= castlingRightsBits;
    arrh[0] ^= ordinal(m_castlingRights);

    return arrh;
}

[[nodiscard]] bool Position::isEpPossible(Square epSquare, Color sideToMove) const
{
    const Bitboard pawnsAttackingEpSquare =
        bb::pawnAttacks(Bitboard::square(epSquare), !sideToMove)
        & piecesBB(Piece(PieceType::Pawn, sideToMove));

    // only set m_epSquare when it matters, ie. when
    // the opposite side can actually capture
    for (Square sq : pawnsAttackingEpSquare)
    {
        if (!BaseType::createsDiscoveredAttackOnOwnKing(Move{ sq, epSquare, MoveType::EnPassant }, sideToMove))
        {
            return true;
        }
    }

    return false;
}

void Position::nullifyEpSquareIfNotPossible()
{
    if (m_epSquare != Square::none() && !isEpPossible(m_epSquare, m_sideToMove))
    {
        m_epSquare = Square::none();
    }
}

namespace detail
{
    [[nodiscard]] FORCEINLINE std::uint8_t compressOrdinaryPiece(const Position&, Square, Piece piece)
    {
        return static_cast<std::uint8_t>(ordinal(piece));
    }

    [[nodiscard]] FORCEINLINE std::uint8_t compressPawn(const Position& position, Square sq, Piece piece)
    {
        const Square epSquare = position.epSquare();
        if (epSquare == Square::none())
        {
            return static_cast<std::uint8_t>(ordinal(piece));
        }
        else
        {
            const Color sideToMove = position.sideToMove();
            const Rank rank = sq.rank();
            const File file = sq.file();
            // use bitwise operators, there is a lot of unpredictable branches but in
            // total the result is quite predictable
            if (
                (file == epSquare.file()) 
                && (
                      ((rank == rank4) & (sideToMove == Color::Black))
                    | ((rank == rank5) & (sideToMove == Color::White))
                  )
               )
            {
                return 12;
            }
            else
            {
                return static_cast<std::uint8_t>(ordinal(piece));
            }
        }
    }

    [[nodiscard]] FORCEINLINE std::uint8_t compressRook(const Position& position, Square sq, Piece piece)
    {
        const CastlingRights castlingRights = position.castlingRights();
        const Color color = piece.color();

        if (color == Color::White
            && (
            (sq == a1 && contains(castlingRights, CastlingRights::WhiteQueenSide))
                || (sq == h1 && contains(castlingRights, CastlingRights::WhiteKingSide))
                )
            )
        {
            return 13;
        }
        else if (
            color == Color::Black
            && (
            (sq == a8 && contains(castlingRights, CastlingRights::BlackQueenSide))
                || (sq == h8 && contains(castlingRights, CastlingRights::BlackKingSide))
                )
            )
        {
            return 14;
        }
        else
        {
            return static_cast<std::uint8_t>(ordinal(piece));
        }
    }

    [[nodiscard]] FORCEINLINE std::uint8_t compressKing(const Position& position, Square sq, Piece piece)
    {
        const Color color = piece.color();
        const Color sideToMove = position.sideToMove();

        if (color == Color::White)
        {
            return 10;
        }
        else if (sideToMove == Color::White)
        {
            return 11;
        }
        else
        {
            return 15;
        }
    }
}

namespace detail::lookup
{
    static constexpr EnumMap<PieceType, std::uint8_t(*)(const Position&, Square, Piece)> pieceCompressorFunc = []() {
        EnumMap<PieceType, std::uint8_t(*)(const Position&, Square, Piece)> pieceCompressorFunc{};

        pieceCompressorFunc[PieceType::Knight] = detail::compressOrdinaryPiece;
        pieceCompressorFunc[PieceType::Bishop] = detail::compressOrdinaryPiece;
        pieceCompressorFunc[PieceType::Queen] = detail::compressOrdinaryPiece;

        pieceCompressorFunc[PieceType::Pawn] = detail::compressPawn;
        pieceCompressorFunc[PieceType::Rook] = detail::compressRook;
        pieceCompressorFunc[PieceType::King] = detail::compressKing;

        pieceCompressorFunc[PieceType::None] = [](const Position&, Square, Piece) -> std::uint8_t { /* should never happen */ return 0; };

        return pieceCompressorFunc;
    }();
}

[[nodiscard]] CompressedPosition Position::compress() const
{
    auto compressPiece = [this](Square sq, Piece piece) -> std::uint8_t {
        if (piece.type() == PieceType::Pawn) // it's likely to be a pawn
        {
            return detail::compressPawn(*this, sq, piece);
        }
        else
        {
            return detail::lookup::pieceCompressorFunc[piece.type()](*this, sq, piece);
        }
    };

    const Bitboard occ = piecesBB();

    CompressedPosition compressed;
    compressed.occupied = occ;

    auto it = occ.begin();
    auto end = occ.end();
    for (int i = 0;; ++i)
    {
        if (it == end) break;
        compressed.packedState[i] = compressPiece(*it, pieceAt(*it));
        ++it;

        if (it == end) break;
        compressed.packedState[i] |= compressPiece(*it, pieceAt(*it)) << 4;
        ++it;
    }

    return compressed;
}

[[nodiscard]] Position CompressedPosition::decompress() const
{
    Position pos;
    pos.setCastlingRights(CastlingRights::None);

    auto decompressPiece = [&pos](Square sq, std::uint8_t nibble) {
        switch (nibble)
        {
        case 0:
        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
        case 8:
        case 9:
        case 10:
        case 11:
        {
            pos.place(fromOrdinal<Piece>(nibble), sq);
            return;
        }

        case 12:
        {
            const Rank rank = sq.rank();
            const File file = sq.file();
            if (rank == rank4)
            {
                pos.place(whitePawn, sq);
                pos.setEpSquareUnchecked(sq + Offset{ 0, -1 });
            }
            else // (rank == rank5)
            {
                pos.place(blackPawn, sq);
                pos.setEpSquareUnchecked(sq + Offset{ 0, 1 });
            }
            return;
        }

        case 13:
        {
            pos.place(whiteRook, sq);
            if (sq == a1)
            {
                pos.addCastlingRights(CastlingRights::WhiteQueenSide);
            }
            else // (sq == H1)
            {
                pos.addCastlingRights(CastlingRights::WhiteKingSide);
            }
            return;
        }

        case 14:
        {
            pos.place(blackRook, sq);
            if (sq == a8)
            {
                pos.addCastlingRights(CastlingRights::BlackQueenSide);
            }
            else // (sq == H8)
            {
                pos.addCastlingRights(CastlingRights::BlackKingSide);
            }
            return;
        }

        case 15:
        {
            pos.place(blackKing, sq);
            pos.setSideToMove(Color::Black);
            return;
        }

        }

        return;
    };

    const Bitboard occ = occupied;

    auto it = occ.begin();
    auto end = occ.end();
    for (int i = 0;; ++i)
    {
        if (it == end) break;
        decompressPiece(*it, packedState[i] & 0xF);
        ++it;

        if (it == end) break;
        decompressPiece(*it, packedState[i] >> 4);
        ++it;
    }

    return pos;
}
