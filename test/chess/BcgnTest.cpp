#include "catch2/catch.hpp"
// again, windows.h has to be included before json.hpp

#include "chess/Bcgn.h"
#include "chess/MoveGenerator.h"

#include <cstdlib>
#include <iostream>

void testBcgnWriter(int seed, std::string filename, bcgn::BcgnFileHeader header, int numGames, bcgn::BcgnFileWriter::FileOpenMode mode = bcgn::BcgnFileWriter::FileOpenMode::Truncate)
{
    srand(seed);

    bcgn::BcgnFileWriter writer(filename, header, mode);

    for (int i = 0; i < numGames; ++i)
    {
        auto pos = Position::startPosition();

        writer.beginGame();

        if (!header.isHeaderless)
        {
            writer.setBlackElo(rand() % 2000 + 1000);
            writer.setWhiteElo(rand() % 2000 + 1000);
            writer.setDate(Date(2020, 4, 17));
            writer.setEco(Eco('E', 1));
            writer.setRound(i % 4000);
            writer.setWhitePlayer("whiteplayer");
            writer.setBlackPlayer("blackplayer");
            writer.setEvent("eventname");
            writer.setSite("sitesitesite");

            if (rand() % 10 == 0)
            {
                writer.setAdditionalTag("additionaltag1", "additionalvalue1");
                writer.setAdditionalTag("additionaltag2", "additionalvalue2");
            }
        }

        if (rand() % 10 == 0)
        {
            writer.setCustomStartPos(pos);
        }

        int movecountInThisGame = 0;
        for (;;)
        {
            if (movecountInThisGame > 100) break;

            const auto moves = movegen::generateLegalMoves(pos);
            if (moves.empty()) break;

            ++movecountInThisGame;
            const auto move = moves[rand() % moves.size()];

            writer.addMove(pos, move);

            pos.doMove(move);
        }

        const auto randomResult = static_cast<GameResult>(rand() % 3);

        writer.setResult(randomResult);

        writer.endGame();
    }
}

void testBcgnReader(int seed, std::string filename, bcgn::BcgnFileHeader header, int numGames, bcgn::BcgnFileWriter::FileOpenMode mode = bcgn::BcgnFileWriter::FileOpenMode::Truncate)
{
    srand(seed);

    bcgn::BcgnFileReader reader(filename);

    int i = 0;
    for (auto& game : reader)
    {
        if (game.hasGameHeader())
        {
            const auto header = game.gameHeader();
            REQUIRE(header.blackElo() == rand() % 2000 + 1000);
            REQUIRE(header.whiteElo() == rand() % 2000 + 1000);
            REQUIRE(header.date() == Date(2020, 4, 17));
            REQUIRE(header.eco() == Eco('E', 1));
            REQUIRE(header.round() == i % 4000);
            REQUIRE(header.whitePlayer() == "whiteplayer");
            REQUIRE(header.blackPlayer() == "blackplayer");
            REQUIRE(header.event() == "eventname");
            REQUIRE(header.site() == "sitesitesite");

            if (rand() % 10 == 0 || true)
            {
                for (auto&& [name, value] : header.additionalTags())
                {
                    if (name == "additionaltag1") REQUIRE(value == "additionalvalue1");
                    if (name == "additionaltag2") REQUIRE(value == "additionalvalue2");
                }
            }
        }

        if (rand() % 10 == 0)
        {
            REQUIRE(game.hasCustomStartPosition());
        }

        auto pos = game.startPosition();
        auto moveProvider = game.moves();
        int movecountInThisGame = 0;
        for (;;)
        {
            if (movecountInThisGame > 100) break;

            const auto moves = movegen::generateLegalMoves(pos);
            if (moves.empty()) break;

            ++movecountInThisGame;
            const auto move = moves[rand() % moves.size()];
            REQUIRE(moveProvider.hasNext());

            const auto providedMove = moveProvider.next(pos);
            REQUIRE(move == providedMove);

            pos.doMove(move);
        }

        const auto readNumPlies = game.numPlies();
        REQUIRE(movecountInThisGame == readNumPlies);

        const auto randomResult = static_cast<GameResult>(rand() % 3);
        const auto readResult = game.result();
        REQUIRE(readResult.has_value());
        REQUIRE(*readResult == randomResult);

        ++i;
    }

    REQUIRE(i == numGames);
}

TEST_CASE("BCGN reader and writer", "[bcgn]") 
{
    constexpr int numGames = 256 * 32;
    constexpr int seed = 12345;

    {
        auto header = bcgn::BcgnFileHeader{};
        header.auxCompression = bcgn::BcgnAuxCompression::None;
        header.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
        header.version = bcgn::BcgnVersion::Version_0;
        header.isHeaderless = false;
        std::cerr << "write test_out/test_v0_c0_ac0.bcgn\n";
        testBcgnWriter(seed, "test_out/test_v0_c0_ac0.bcgn", header, numGames);
        std::cerr << "read test_out/test_v0_c0_ac0.bcgn\n";
        testBcgnReader(seed, "test_out/test_v0_c0_ac0.bcgn", header, numGames);
    }

    {
        auto header = bcgn::BcgnFileHeader{};
        header.auxCompression = bcgn::BcgnAuxCompression::None;
        header.compressionLevel = bcgn::BcgnCompressionLevel::Level_1;
        header.version = bcgn::BcgnVersion::Version_0;
        header.isHeaderless = false;
        std::cerr << "write test_out/test_v0_c1_ac0.bcgn\n";
        testBcgnWriter(seed, "test_out/test_v0_c1_ac0.bcgn", header, numGames);
        std::cerr << "read test_out/test_v0_c1_ac0.bcgn\n";
        testBcgnReader(seed, "test_out/test_v0_c1_ac0.bcgn", header, numGames);
    }

    {
        auto header = bcgn::BcgnFileHeader{};
        header.auxCompression = bcgn::BcgnAuxCompression::None;
        header.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
        header.version = bcgn::BcgnVersion::Version_0;
        header.isHeaderless = true;
        std::cerr << "write test_out/test_v0_c0_ac0_headerless.bcgn\n";
        testBcgnWriter(seed, "test_out/test_v0_c0_ac0_headerless.bcgn", header, numGames);
        std::cerr << "read test_out/test_v0_c0_ac0_headerless.bcgn\n";
        testBcgnReader(seed, "test_out/test_v0_c0_ac0_headerless.bcgn", header, numGames);
    }

    {
        auto header = bcgn::BcgnFileHeader{};
        header.auxCompression = bcgn::BcgnAuxCompression::None;
        header.compressionLevel = bcgn::BcgnCompressionLevel::Level_1;
        header.version = bcgn::BcgnVersion::Version_0;
        header.isHeaderless = true;
        std::cerr << "write test_out/test_v0_c1_ac0_headerless.bcgn\n";
        testBcgnWriter(seed, "test_out/test_v0_c1_ac0_headerless.bcgn", header, numGames);
        std::cerr << "read test_out/test_v0_c1_ac0_headerless.bcgn\n";
        testBcgnReader(seed, "test_out/test_v0_c1_ac0_headerless.bcgn", header, numGames);
    }

    {
        auto header = bcgn::BcgnFileHeader{};
        header.auxCompression = bcgn::BcgnAuxCompression::None;
        header.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
        header.version = bcgn::BcgnVersion::Version_0;
        std::cerr << "write test_out/test_append.bcgn\n";
        testBcgnWriter(seed, "test_out/test_append.bcgn", header, numGames);
        std::cerr << "append test_out/test_append.bcgn\n";
        testBcgnWriter(seed, "test_out/test_append.bcgn", header, numGames, bcgn::BcgnFileWriter::FileOpenMode::Append);
    }
}