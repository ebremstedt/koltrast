
import unittest
from koltrast.chunks import Chunk, parse_chunk

class TestChunk(unittest.TestCase):
    def test_is_valid(self):
        self.assertTrue(Chunk.is_valid("day"))
        self.assertTrue(Chunk.is_valid("hour"))
        self.assertTrue(Chunk.is_valid("week"))
        self.assertTrue(Chunk.is_valid("month"))
        self.assertTrue(Chunk.is_valid("year"))
        self.assertFalse(Chunk.is_valid("invalid"))

    def test_parse_chunk_valid(self):
        self.assertEqual(parse_chunk("day"), Chunk.DAY)
        self.assertEqual(parse_chunk("days"), Chunk.DAY)
        self.assertEqual(parse_chunk("hour"), Chunk.HOUR)
        self.assertEqual(parse_chunk("hours"), Chunk.HOUR)
        self.assertEqual(parse_chunk("week"), Chunk.WEEK)
        self.assertEqual(parse_chunk("weeks"), Chunk.WEEK)
        self.assertEqual(parse_chunk("month"), Chunk.MONTH)
        self.assertEqual(parse_chunk("months"), Chunk.MONTH)
        self.assertEqual(parse_chunk("year"), Chunk.YEAR)
        self.assertEqual(parse_chunk("years"), Chunk.YEAR)

    def test_parse_chunk_invalid(self):
        with self.assertRaises(ValueError):
            parse_chunk("invalid")


if __name__ == '__main__':
    unittest.main()
