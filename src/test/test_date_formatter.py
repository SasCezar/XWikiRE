import unittest

from date_parsers import DateFormatter, RomanLanguageDateFormatter


class TestFormatterMethods(unittest.TestCase):
    def test_millennium(self):
        formatter = DateFormatter()
        precision = 6
        date = "+0000020000-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "21st millennium")
        date = "-00000002000-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "3rd millennium BC")

    def test_century(self):
        formatter = DateFormatter()
        precision = 7

        date = "+00000001900-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "20th century")
        date = "-00000001900-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "20th century BC")

    def test_year(self):
        formatter = DateFormatter()
        precision = 9

        date = "+00000001920-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "1920")
        date = "-00000001920-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "1920 BC")

    def test_month(self):
        formatter = DateFormatter()
        precision = 10

        date = "+00000001920-01-00T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "January 1920")
        date = "-000000020-01-00T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "January 20 BC")

    def test_day(self):
        formatter = DateFormatter()
        precision = 11

        date = "+00000001920-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "1 January 1920")
        date = "-000000020-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "1 January 20 BC")

    def test_month_french(self):
        formatter = DateFormatter(lang='fr', out_locale='fr-FR')
        precision = 10

        date = "+00000001920-01-02T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "janvier 1920")
        date = "-000000020-01-10T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "janvier 20 J.-C")

    def test_month_italian(self):
        formatter = DateFormatter(lang='it', out_locale='it-IT')
        precision = 10

        date = "+00000001920-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "gennaio 1920")
        date = "-000000020-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "gennaio 20 a.C.")


class TestRomanFormatterMethods(unittest.TestCase):
    def test_millennium(self):
        formatter = RomanLanguageDateFormatter()
        precision = 6
        date = "+0000020000-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "XXI millennium")
        date = "-00000002000-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "III millennium BC")

    def test_century(self):
        formatter = RomanLanguageDateFormatter()
        precision = 7

        date = "+00000001900-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "XX century")
        date = "-00000001900-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "XX century BC")

    def test_millenium_italian(self):
        formatter = RomanLanguageDateFormatter(lang="it", out_locale='it-IT')
        precision = 6
        date = "+0000020000-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "XXI millennio")
        date = "-00000002000-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "III millennio a.C.")

    def test_century_french(self):
        formatter = RomanLanguageDateFormatter(lang="fr", out_locale='fr-FR')
        precision = 7
        date = "+00000001900-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "XXe siècle")
        date = "-0000000800-01-01T00:00:00Z"
        formatted = formatter.format(date, precision)
        self.assertEquals(formatted, "IXe siècle J.-C")


if __name__ == '__main__':
    unittest.main()
