#pragma once

#include "Chess.h"
#include "Position.h"

#include <string>

// Extended Reversible Algebraic Notation
// Just like Reversible Algebraic Notation (RAN), but also includes
// Old castling rights and old en-passant square at the end
// in the order and format like in FEN.
namespace eran
{
    [[nodiscard]] std::string reverseMoveToEran(const Position& pos, const ReverseMove& rmove);
}
