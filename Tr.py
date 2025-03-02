from googletrans import Translator  


def translator(text):
    translator = Translator()  
    translated = translator.translate(text, src='fr', dest='en')  
    return translated.text

text = "Travaux de réalisation de vingt (20) forages dont dix (10) productifs à débit supérieur ou égal à 5m3/h à équiper de Système d’Hydraulique Pastorale Amélioré (SHPA)"  
print(translator(text))