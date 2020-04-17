#include "chess/Bcgn.h"
#include "chess/MoveGenerator.h"

#include <cstdlib>
#include <iostream>

void testBcgnWriter(int seed, std::string filename, bcgn::BcgnHeader options, int numGames, bcgn::BcgnWriter::FileOpenMode mode = bcgn::BcgnWriter::FileOpenMode::Truncate)
{
    srand(seed);

    bcgn::BcgnWriter writer(filename, options, mode);

    for (int i = 0; i < numGames; ++i)
    {
        auto pos = Position::startPosition();

        writer.beginGame();

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

void testBcgnReader(int seed, std::string filename, bcgn::BcgnHeader options, int numGames, bcgn::BcgnWriter::FileOpenMode mode = bcgn::BcgnWriter::FileOpenMode::Truncate)
{
    srand(seed);

    bcgn::BcgnReader reader(filename);

    int i = 0;
    for (auto& game : reader)
    {
        TEST_ASSERT(game.blackElo() == rand() % 2000 + 1000);
        TEST_ASSERT(game.whiteElo() == rand() % 2000 + 1000);
        TEST_ASSERT(game.date() == Date(2020, 4, 17));
        TEST_ASSERT(game.eco() == Eco('E', 1));
        TEST_ASSERT(game.round() == i % 4000);
        TEST_ASSERT(game.whitePlayer() == "whiteplayer");
        TEST_ASSERT(game.blackPlayer() == "blackplayer");
        TEST_ASSERT(game.event() == "eventname");
        TEST_ASSERT(game.site() == "sitesitesite");

        if (rand() % 10 == 0)
        {
            TEST_ASSERT(game.getAdditionalTagValue("additionaltag1") == "additionalvalue1");
            TEST_ASSERT(game.getAdditionalTagValue("additionaltag2") == "additionalvalue2");
        }

        if (rand() % 10 == 0)
        {
            TEST_ASSERT(game.hasCustomStartPosition());
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
            TEST_ASSERT(moveProvider.hasNext());

            const auto providedMove = moveProvider.next(pos);
            TEST_ASSERT(move == providedMove);

            pos.doMove(move);
        }

        const auto readNumPlies = game.numPlies();
        TEST_ASSERT(movecountInThisGame == readNumPlies);

        const auto randomResult = static_cast<GameResult>(rand() % 3);
        const auto readResult = game.result();
        TEST_ASSERT(readResult.has_value());
        TEST_ASSERT(*readResult == randomResult);

        ++i;
    }

    TEST_ASSERT(i == numGames);
}

void testBcgnWriter()
{
    constexpr int numGames = 256 * 32;
    constexpr int seed = 12345;

    {
        auto options = bcgn::BcgnHeader{};
        options.auxCompression = bcgn::BcgnAuxCompression::None;
        options.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
        options.version = bcgn::BcgnVersion::Version_0;
        std::cerr << "write test_out/test_v0_c0_ac0.bcgn\n";
        testBcgnWriter(seed, "test_out/test_v0_c0_ac0.bcgn", options, numGames);
        std::cerr << "read test_out/test_v0_c0_ac0.bcgn\n";
        testBcgnReader(seed, "test_out/test_v0_c0_ac0.bcgn", options, numGames);
    }

    {
        auto options = bcgn::BcgnHeader{};
        options.auxCompression = bcgn::BcgnAuxCompression::None;
        options.compressionLevel = bcgn::BcgnCompressionLevel::Level_1;
        options.version = bcgn::BcgnVersion::Version_0;
        std::cerr << "write test_out/test_v0_c1_ac0.bcgn\n";
        testBcgnWriter(seed, "test_out/test_v0_c1_ac0.bcgn", options, numGames);
        std::cerr << "read test_out/test_v0_c1_ac0.bcgn\n";
        testBcgnReader(seed, "test_out/test_v0_c1_ac0.bcgn", options, numGames);
    }

    {
        auto options = bcgn::BcgnHeader{};
        options.auxCompression = bcgn::BcgnAuxCompression::None;
        options.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
        options.version = bcgn::BcgnVersion::Version_0;
        std::cerr << "write test_out/test_append.bcgn\n";
        testBcgnWriter(seed, "test_out/test_append.bcgn", options, numGames);
        std::cerr << "append test_out/test_append.bcgn\n";
        testBcgnWriter(seed, "test_out/test_append.bcgn", options, numGames, bcgn::BcgnWriter::FileOpenMode::Append);
    }
}