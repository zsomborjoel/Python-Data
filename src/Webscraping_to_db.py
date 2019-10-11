from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup, NavigableString, Tag
from time import sleep
from random import randint
import pyodbc
import time
import datetime
import re

def simple_get(url):
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        pass


def is_good_response(resp):
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > 1)


def log_error(e):
    errorlogdate = time.strftime("%Y%m%d")
    errorlog = open("Error_Log_" + errorlogdate + ".txt", "a+")
    errorlog.write(e + "\n")
    errorlog.close()


def product_insert():

    urls = open("urls.txt").read().splitlines()
    runloop = 0

    # Adatbázis kapcsolat
    server = '192.168.0.80,2000'
    database = 'COMP_DB'
    username = 'wntzsombi'
    password = 'pass'
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cur = conn.cursor()

    for x in range(len(urls)):
        try:
            raw_html = simple_get(urls[x])
            soup = BeautifulSoup(raw_html, 'html.parser')
            products = []
            products_desc = []
            prices = []

            #Termék
            for a in soup.find_all('a', href=True, text=True):
                link = a['href']
                text = a.next
                if str(link)[23:30] == 'product':
                    product = text
                    cleanproduct = product.replace("'", "")
                    products.append("'" + cleanproduct.strip(" ") + "'")

            #Leírás
            for a in soup.find_all('a', href=True, text=True):
                link = a['href']
                if str(link)[23:30] == 'product':
                    next_s = a.nextSibling
                    next2_s = next_s.nextSibling
                    next3_s = next2_s.nextSibling
                    clean_desc = next3_s.replace("'", "")
                    products_desc.append("'" + clean_desc.strip(" ") + "'")

            #Árak
            columns = soup.findAll('td', text=re.compile('Ft'))
            for i in columns:
                price = i.text
                preclean = price.replace("Ft", "")
                cleanprice = preclean.replace(",", "")
                prices.append("'" + cleanprice.strip(" ") + "'")

            if urls[x][39:42] == '167':
                z = 'ABLAKTÖLŐ LAPÁTOK'
            elif urls[x][39:41] == '91':
                z = 'ABS SENZOROK'
            elif urls[x][39:42] == '118':
                z = 'AGR,EGR SZELEPEK'
            elif urls[x][39:42] == '154':
                z = 'AKCIÓS AGIP MOTOROLAJOK'
            elif urls[x][39:42] == '105':
                z = 'AKCIÓS CASTROL MOTORKERÉKPÁR OLAJOK'
            elif urls[x][39:42] == '15 ':
                z = 'AKCIÓS AGIP MOTOROLAJOK'
            elif urls[x][39:42] == '137':
                z = 'AKCIÓS ELF MOTOROLAJOK'
            elif urls[x][39:42] == '104':
                z = 'AKCIÓS FORD MOTOROLAJOK'
            elif urls[x][39:42] == '103':
                z = 'AKCIÓS GM-OPEL MOTOROLAJOK'
            elif urls[x][39:42] == '102':
                z = 'AKCIÓS IGOL MOTOROLAJOK'
            elif urls[x][39:42] == '185':
                z = 'AKCIÓS MAZDA MOTOROLAJOK'
            elif urls[x][39:42] == '101':
                z = 'AKCIÓS MOBIL MOTOROLAJOK'
            elif urls[x][39:42] == '173':
                z = 'AKCIÓS MOL MOTOROLAJOK'
            elif urls[x][39:42] == '139':
                z = 'AKCIÓS MOTUL MOTORKERÉKPÁR OLAJOK'
            elif urls[x][39:42] == '109':
                z = 'AKCIÓS Q8 MOTOROLAJOK'
            elif urls[x][39:42] == '135':
                z = 'AKCIÓS TOTAL MOTOROLAJOK'
            elif urls[x][39:41] == '99':
                z = 'AKCIÓS REPSOL MOTORKERÉKPÁR OLAJOK'
            elif urls[x][39:41] == '98':
                z = 'AKCIÓS SHELL MOTORKERÉKPÁR OLAJOK'
            elif urls[x][39:41] == '97':
                z = 'AKCIÓS SHELL MOTOROLAJOK'
            elif urls[x][39:41] == '85':
                z = 'AKKUMULÁTOROK'
            elif urls[x][39:41] == '36':
                z = 'ALAPJÁRATI MOTOROK FOJTÓSZELEPHÁZAK'
            elif urls[x][39:41] == '23':
                z = 'ÁLTALÁNOS KIEGÉSZÍTŐK,FELSZERELÉSI CIKKEK'
            elif urls[x][39:44] == '23_148':
                z = 'ELEKTROMOS ALKATRÉSZEK'
            elif urls[x][39:44] == '23_157':
                z = 'GÉGECSÖVEK'
            elif urls[x][39:44] == '23_166':
                z = 'HÓLÁNCOK'
            elif urls[x][39:44] == '23_186':
                z = 'KULCSTARTÓK'
            elif urls[x][39:44] == '23_147':
                z = 'VÍZCSÖVEK,VÍZCSŐTOLDÓK'
            elif urls[x][39:42] == '164':
                z = 'BARKAS ALKATRÉSZEK'
            elif urls[x][39:42] == '113':
                z = 'BILINCSEK'
            elif urls[x][39:41] == '87':
                z = 'BIZTOSITÉKOK,BIZTOSÍTÉK TARTÓK'
            elif urls[x][39:42] == '176':
                z = 'CATACLEAN KATALIZÁTOR TISZTÍTÓ'
            elif urls[x][39:42] == '170':
                z = 'CSOMAGTÉRTÁLCÁK, CSOMAGTÉRVÉDŐK MINDEN TÍPUSHOZK'
            elif urls[x][39:42] == '152':
                z = 'DAEWOO,CHEVROLET ALKATRÉSZEK'
            elif urls[x][39:42] == '184':
                z = 'DIESEL ADAGOLÓ-BEFECSKENDEZŐ,TANDEMPUMPA ALKATRÉSZEK'
            elif urls[x][39:42] == '165':
                z = 'DÍSZTÁRCSÁK'
            elif urls[x][39:42] == '180':
                z = 'DOBBETÉT BOLHÁK,DOBBETÉT PATENTEK,KÁRPITBOLHÁK,KÁRPITPATENTEK'
            elif urls[x][39:42] == '125':
                z = 'EGYÉB OLAJOK,EGYÉB MOTOROLAJOK'
            elif urls[x][39:41] == '38':
                z = 'ELEKTROMOS AC PUMPÁK'
            elif urls[x][39:42] == '178':
                z = 'GYÚJTÓKÁBELEK,GYERTYAKÁBELEK,TRAFÓKÁBELEK,KÁBELVÉGEK'
            elif urls[x][39:41] == '49':
                z = 'IZZÓK,FOGLALATOK'
            elif urls[x][39:41] == '95':
                z = 'ELEMEK'
            elif urls[x][39:41] == '93':
                z = 'KAPCSOLÓK NYOMÓGOMBOK'
            elif urls[x][39:42] == '126':
                z = 'KATALIZÁTOROK'
            elif urls[x][39:42] == '130':
                z = 'KEIKO SZERVÓ SZIVATTYÚK,VÁKUMPUMPÁK'
            elif urls[x][39:42] == '110':
                z = 'KERÉKCSAVAROK, KERÉKANYÁK, KERÉKŐRÖK'
            elif urls[x][39:42] == '169':
                z = 'KIPUFOGÓ DOBOK,CSÖVEK MINDEN TÍPUSHOZ'
            elif urls[x][39:42] == '116':
                z = 'KIPUFOGÓ FLEXIBILIS CSÖVEK,TOLDÓK-CSATLAKOZÓK, KIPUFOGÓ VÉGEK,KIPUFOGÓGÁZ GYORSÍTÓK'
            elif urls[x][39:42] == '146':
                z = 'KIPUFOGÓTARTÓ GUMIK'
            elif urls[x][39:41] == '94':
                z = 'KONTROL LÁMPÁK'
            elif urls[x][39:41] == '53':
                z = 'LADA SZAMARA ALKATRÉSZEK'
            elif urls[x][39:44] == '53_58':
                z = 'LADA SZAMARA ELEKTROMOS ALKATRÉSZEK'
            elif urls[x][39:44] == '53_77':
                z = 'LADA SZAMARA FÉK ALKATRÉSZEK'
            elif urls[x][39:44] == '53_67':
                z = 'LADA SZAMARA FUTÓMŰ ALKATRÉSZEK'
            elif urls[x][39:44] == '53_59':
                z = 'LADA SZAMARA KAROSSZÉRIA ALKATRÉSZEK'
            elif urls[x][39:44] == '53_57':
                z = 'LADA SZAMARA MOTOR ALKATRÉSZEK'
            elif urls[x][39:44] == '53_78':
                z = 'LADA SZAMARA SEBESSÉGVÁLTÓ ALKATRÉSZEK'
            elif urls[x][39:41] == '29':
                z = 'LADA NIVA ALKATRÉSZEK'
            elif urls[x][39:44] == '29_55':
                z = 'LADA NIVA ELEKTROMOS ALKATRÉSZEKK'
            elif urls[x][39:44] == '29_62':
                z = 'LADA NIVA FÉK ALKATRÉSZEK'
            elif urls[x][39:44] == '29_64':
                z = 'LADA NIVA FUTÓMŰ ALKATRÉSZEK'
            elif urls[x][39:44] == '29_56':
                z = 'LADA NIVA KAROSSZÉRIA ALKATRÉSZEK'
            elif urls[x][39:44] == '29_54':
                z = 'LADA NIVA MOTOR ALKATRÉSZEK'
            elif urls[x][39:44] == '29_55':
                z = 'LADA NIVA ELEKTROMOS ALKATRÉSZEK'
            elif urls[x][39:44] == '29_62':
                z = 'LADA NIVA FÉK ALKATRÉSZEK'
            elif urls[x][39:44] == '29_64':
                z = 'LADA NIVA FUTÓMŰ ALKATRÉSZEK'
            elif urls[x][39:44] == '29_56':
                z = 'LADA NIVA KAROSSZÉRIA ALKATRÉSZEK'
            elif urls[x][39:44] == '29_54':
                z = 'LADA NIVA MOTOR ALKATRÉSZEK'
            elif urls[x][39:44] == '29_63':
                z = 'LADA NIVA SEBESSÉGVÁLTÓ ALKATRÉSZEK'
            elif urls[x][39:41] == '37':
                z = 'LAMBDASZONDÁK'
            elif urls[x][39:42] == '133':
                z = 'LEDEK'
            elif urls[x][39:41] == '39':
                z = 'LÉGTÖMEGMÉRŐK'
            elif urls[x][39:42] == '182':
                z = 'MATRICÁK'
            elif urls[x][39:42] == '106':
                z = 'MOTOR-LIFE'
            elif urls[x][39:42] == '175':
                z = 'NAPPALI MENETFÉNYEK'
            elif urls[x][39:42] == '149':
                z = 'OLAJLEERESZTŐ CSAVAROK'
            elif urls[x][39:41] == '35':
                z = 'OPEL ALKATRÉSZEK'
            elif urls[x][39:42] == '182':
                z = 'OSRAM NIGHT BREAKER LASER IZZÓK'
            elif urls[x][39:41] == '25':
                z = 'OSRAM NIGHTBREAKER PLUS,UNLIMITED OSRAM NIGHT BREAKER PLUS,UNLIMITED'
            elif urls[x][39:42] == '151':
                z = 'PARAGI SPORT KIPUFOGÓDOBOK'
            elif urls[x][39:41] == '32':
                z = 'POLSKI FIAT 126P ALKATRÉSZEK'
            elif urls[x][39:42] == '174':
                z = 'PRO-TEC TERMÉKEK'
            elif urls[x][39:42] == '143':
                z = 'SIKA TERMÉKEK'
            elif urls[x][39:41] == '31':
                z = 'SKODA ALKATRÉSZEK'
            elif urls[x][39:41] == '28':
                z = 'SUZUKI , MARUTI ALKATRÉSZEK'
            elif urls[x][39:41] == '22':
                z = 'SZERSZÁMOK'
            elif urls[x][39:41] == '33':
                z = 'SZŰRŐK'
            elif urls[x][39:42] == '134':
                z = 'TACHOGRÁF PAPÍROK,KORONGOK'
            elif urls[x][39:41] == '30':
                z = 'TRABANT-WARTBURG ALKATRÉSZEK'
            elif urls[x][39:41] == '40':
                z = 'TRAFÓK , HALLJELADÓK'
            elif urls[x][39:41] == '92':
                z = 'TURBO NYOMÁS SZABÁLYZÓ SZELEPEK'
            elif urls[x][39:42] == '171':
                z = 'ÜLÉSHUZATOK TRIKO'
            elif urls[x][39:42] == '141':
                z = 'UTÁNFUTÓ TARTOZÉKOK,FELSZERELÉSEK'
            elif urls[x][39:42] == '142':
                z = 'ÜZEMANYAGCSÖVEK,BENZINCSÖVEK, MŰSZAKICSÖVEK'
            elif urls[x][39:41] == '34':
                z = 'VEGYIÁRUK , KEMIKÁLIÁK'
            elif urls[x][39:42] == '153':
                z = 'VOLKSWAGEN ALKATRÉSZEK'
            elif urls[x][39:42] == '111':
                z = 'VOLKSWAGEN SKODA TURBO'
            elif urls[x][39:42] == '150':
                z = 'WUNDERBAUM ILLATOSÍTÓK'
            elif urls[x][39:42] == '119':
                z = 'XADO TERMÉKEK'
            elif urls[x][39:42] == '81':
                z = 'XENON IZZÓK /D1S, D3S, D2R,D2S/, XENON SZETTEK'
            else:
                z = 'EGYÉB TERMÉK'


            #Listák összevonása
            final_list = [data for ij in zip(products, products_desc, prices) for data in ij]

            #feldarabolás
            chunks = [final_list[x:x + 3] for x in range(0, len(final_list), 3)]

            #Inserthez szükséges forma kialakítása
            sql = ["('2', 'Shop', '" + z + "', " + ", ".join(a) + ")" for a in chunks]

            #Végleges insert létrehozása
            if len(chunks) == 0:
                insert = "INSERT INTO termék_árak (cég_kód, cég_név, kategória, termék_név, termék_leírás, termék_ár) VALUES ('2', 'Báróker', '" + z + "', 'Nincs online termék', '', '0');"
            else:
                insert = "INSERT INTO termék_árak (cég_kód, cég_név, kategória, termék_név, termék_leírás, termék_ár) VALUES %s" % ("," .join(sql))

            #Betöltés, és comitálás
            cur.execute(insert)
            conn.commit()

            runloop = runloop + 1
            running_info = str(runloop) + "/439"
            print(running_info)

            #random idő várakoztatás
            sleep(randint(10,20))

        except Exception as e:
            log_error("Url száma: " + running_info + " - Error üzenet: " + str(e))
            pass

    conn.close()

def main():

    start_time = time.time()
    product_insert()
    runtime = (time.time() - start_time) / 60
    currenttime = datetime.datetime.now()
    logdate = time.strftime("%Y%m%d")

    runlog = open("Scrape_Log_" + logdate + ".txt", "w")
    message = "Futás befejeződött " + str(currenttime) + " kor. A Futási idő " + str(runtime) + " perc volt."
    runlog.write(message)
    print("A töltés befejeződött.")

if __name__ == '__main__':
    main()






