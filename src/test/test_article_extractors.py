import unittest

from article_extractors import ItalianArticleExtractor


class TestItalianArticleExtractor(unittest.TestCase):
    def test_none(self):
        extractor = ItalianArticleExtractor()

        text = """Barack Hussein Obama II (/bəˈrɑːk hʊˈseɪn oʊˈbɑːmə/, pronuncia[?·info]; Honolulu, 4 agosto 1961) è 
        un politico statunitense, 44º presidente degli Stati Uniti d'America dal 2009 al 2017, prima persona di 
        origini afroamericane a ricoprire tale carica. 

Figlio di un'antropologa originaria del Kansas e di un economista kenyota, Obama si è laureato in scienze politiche 
alla Columbia University (1983) e in giurisprudenza alla Harvard Law School (1991), dove è stato la prima persona di 
colore a dirigere la rivista Harvard Law Review. Prima di portare a termine gli studi in legge, ha prestato la sua 
opera come «community organizer» a Chicago; successivamente ha lavorato come avvocato nel campo della difesa dei 
diritti civili, insegnando inoltre diritto costituzionale presso la Law School dell'Università di Chicago dal 1992 al 
2004. 

Barack Obama è stato membro del Senato dell'Illinois per tre mandati, dal 1997 al 2004. Dopo essersi candidato senza 
successo alla Camera dei rappresentanti nel 2000, quattro anni più tardi concorse per il Senato federale, imponendosi 
a sorpresa nelle primarie del Partito Democratico del marzo 2004 su di un folto gruppo di contendenti. L'inopinata 
vittoria alle primarie contribuì ad accrescere la sua notorietà; in seguito, il suo discorso introduttivo («keynote 
address») pronunciato in occasione della convention democratica di luglio lo rese una delle figure più eminenti del 
suo partito. Obama fu quindi eletto al Senato degli Stati Uniti nel novembre 2004, con il più ampio margine nella 
storia dell'Illinois, e prestò servizio come senatore junior dal gennaio 2005 al novembre 2008. 

Il 10 febbraio 2007 annunciò ufficialmente la propria candidatura alle successive consultazioni presidenziali.[1] 
Alle elezioni primarie del Partito Democratico, dopo un'aspra contesa, sconfisse Hillary Clinton, senatrice in carica 
per lo Stato di New York e già first lady, favorita della vigilia; il 3 giugno 2008 Obama raggiunse il quorum 
necessario per la candidatura, divenendo così la prima persona di origini afroamericane a correre per la Casa Bianca 
in rappresentanza di uno dei due maggiori partiti. 

L'esponente del Partito Democratico vinse le elezioni presidenziali del 4 novembre 2008 contro John McCain, 
senatore repubblicano dell'Arizona, insediandosi formalmente alla presidenza il 20 gennaio successivo. Il 6 novembre 
2012 fu riconfermato per un secondo mandato, imponendosi sul candidato repubblicano Mitt Romney. 

Il settimanale statunitense TIME lo ha prescelto quale «persona dell'anno» nel 2008[2] e nel 2012;[3] nel 2009 è 
stato insignito del Premio Nobel per la pace «per i suoi sforzi straordinari volti a rafforzare la diplomazia 
internazionale e la cooperazione tra i popoli».[4] """

        article = extractor.extract(text, "Barack Obama")

        self.assertEqual("", article)

    def test_yes(self):
        extractor = ItalianArticleExtractor()

        text = """I Girasoli sono una serie di dipinti ad olio su tela realizzati tra il 1888 e il 1889 dal pittore 
        Vincent van Gogh. Tra i soggetti preferiti dal pittore, sono oggi tra le sue opere più riconoscibili e note 
        presso il grande pubblico. """
        article = extractor.extract(text, "Girasoli")

        self.assertEqual("I", article)

    def test_in(self):
        extractor = ItalianArticleExtractor()

        text = """La bella e la bestia (Beauty and the Beast) è un film del 2017 diretto da Bill Condon.

Scritto da Evan Spiliotopoulos e Stephen Chbosky, il film è un remake in live action dell'omonimo film d'animazione 
del 1991, tratto dalla fiaba di Jeanne-Marie Leprince de Beaumont. Il film è interpretato da un cast corale che 
comprende Emma Watson, Dan Stevens, Luke Evans, Kevin Kline, Josh Gad, Ewan McGregor, Stanley Tucci, Ian McKellen ed 
Emma Thompson. 

È il primo film Disney in cui compare un personaggio omosessuale: si tratta di Le Tont, interpretato da Josh Gad.[1]"""
        article = extractor.extract(text, "La bella e la bestia")

        self.assertEqual("La", article)
