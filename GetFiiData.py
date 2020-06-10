import io
import sys
import csv
import requests
import threading
import time
from lxml import html
import fii

# Following lines are not needed when running with Python 3
# reload(sys)
# sys.setdefaultencoding('UTF8')

fieldNames = [
        'Sigla',                #0
        'Nome',                 #1
        'Tipo',                 #2
        'Dividendo (%)',        #3
        'Patrimônio (R$)',      #4
        'VPA (R$)',             #5
        'V/VPA',                #6
        'Valor (R$)',           #7
        'Cotas',                #8
        'Max52 (R$)',           #9
        'Min52 (R$)',           #10
        'Valorização (%/ano)']  #11

class RealEstateInvestmentFund:
    def __init__(self, name):
        self.name = name
        self.url = fii.LINK + self.name.lower()
        self.page = requests.get(self.url)
        self.tree = html.fromstring(self.page.content)

    def processValue(self, result):
        if len(result) > 0:
            string = str(result[0])
            string = string.replace(".", "")
            string = string.replace(",", ".")
            string = string.replace("R$", "")
            string = string.strip()
            return string
        return ""

    def getFiiName(self):
        result = self.tree.xpath(fii.FII_NAME)
        return self.processValue(result)

    def getFiiType(self):
        result = self.tree.xpath(fii.FII_TYPE)
        return self.processValue(result)

    def getDividendYield(self):
        result = self.tree.xpath(fii.DIVIDEND_YIELD)
        return self.processValue(result)

    def getAssetValue(self):
        result = self.tree.xpath(fii.ASSET_VALUE)
        return self.processValue(result)

    def getAssetValuePerShare(self):
        result = self.tree.xpath(fii.ASSET_VALUE_PER_SHARE)
        return self.processValue(result)

    def getVVPA(self):
        try:
            value = self.getValue()
            assetValue = self.getAssetValuePerShare()
            return float(value)/float(assetValue)
        except:
            return -1

    def getValue(self):
        result = self.tree.xpath(fii.VALUE)
        return self.processValue(result)

    def getNumberOfShares(self):
        result = self.tree.xpath(fii.NUMBER_SHARES)
        return self.processValue(result)

    def getMax52Weeks(self):
        result = self.tree.xpath(fii.MAX_52WEEK)
        return self.processValue(result)

    def getMin52Weeks(self):
        result = self.tree.xpath(fii.MIN_52WEEK)
        return self.processValue(result)

    def getAppreciation(self):
        result = self.tree.xpath(fii.APPRECIATION)
        return self.processValue(result)

class ProbeDataThread (threading.Thread):
   def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.dict = {}

   def run(self):
        print("Starting thread for " + self.name)
        self.fii = RealEstateInvestmentFund(self.name)
        self.dict[fieldNames[0]] = self.fii.name
        self.dict[fieldNames[1]] = self.fii.getFiiName()
        self.dict[fieldNames[2]] = self.fii.getFiiType()
        self.dict[fieldNames[3]] = self.fii.getDividendYield()
        self.dict[fieldNames[4]] = self.fii.getAssetValue()
        self.dict[fieldNames[5]] = self.fii.getAssetValuePerShare()
        self.dict[fieldNames[6]] = self.fii.getVVPA()
        self.dict[fieldNames[7]] = self.fii.getValue()
        self.dict[fieldNames[8]] = self.fii.getNumberOfShares()
        self.dict[fieldNames[9]] = self.fii.getMax52Weeks()
        self.dict[fieldNames[10]] = self.fii.getMin52Weeks()
        self.dict[fieldNames[11]] = self.fii.getAppreciation()

        print("================================================")
        print("Sigla do Fundo: " + self.dict[fieldNames[0]])
        print("Nome do Fundo: " + self.dict[fieldNames[1]])
        print("Tipo do Fundo: " + self.dict[fieldNames[2]])
        print("Dividend yield: " + self.dict[fieldNames[3]] + "%")
        print("Valor patrimonial: " + "R$ " + self.dict[fieldNames[4]])
        print("Valor patrimonial por cota: " + "R$ " + self.dict[fieldNames[5]])
        print("V/VPA: " + str(self.dict[fieldNames[6]]))
        print("Valor por cota: " + "R$ " + self.dict[fieldNames[7]])
        print("Total de cotas: " + self.dict[fieldNames[8]])
        print("Max 52 semanas: " + "R$ " + self.dict[fieldNames[9]])
        print("Min 52 semanas: " + "R$ " + self.dict[fieldNames[10]])
        print("Valorização: " + self.dict[fieldNames[11]] + "%")
        print("================================================")

        print("Finishing thread for " + self.name)

# Create threads to probe funds data
threadList = []
for v in fii.FII:
    thread = ProbeDataThread(v, v)
    thread.start()
    threadList.append(thread)
    time.sleep(0.2)

# Wait for all threads to complete
for t in threadList:
    t.join()

# Save data
with io.open('Fii.csv', 'w', encoding='utf-8') as csvFile:
    writer = csv.DictWriter(csvFile, fieldnames=fieldNames)
    writer.writeheader()
    for thread in threadList:
        writer.writerow(thread.dict)
