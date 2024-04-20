import time
import NSEDownload.stocks as stocks
import unittest
import numpy as np
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import calendar
import threading
import queue

def open_connection():
    # mysql connection 
    mydb = mysql.connector.connect(
    host="localhost", #intelora.co.in
    user="root",   #intelaku_shivam
    password="Root", #intelaku_shivam
    database="stockmarket" #intelaku_shivam
    )
    return mydb
 
# mydb = mysql.connector.connect(
#   host="intelora.co.in",
#   user="intelaku_shivam",
#   password="intelaku_shivam",
#   database="intelaku_shivam"
# )


# def renameDataframeExample(dataframe):
#     dataframe.rename(columns = {
#         'Date':'tradedate', 
#         'High Price':'high', 
#         'Low Price':'low', 
#         'Open Price':'open', 
#         'Close Price': 'close',
#         'Total Traded Quantity': 'quantity',
#         'Total Traded Value': 'tradeValue',
#         '52 Week High Price': '52w_high',
#         '52 Week Low Price': '52W_low',
#         'Last Price':'last',
#         'Prev Close Price':'prev_close'        
#     }, inplace = True) 

toReset=True

all_scripts = [
    "20MICRONS", "21STCENMGM", "3IINFOTECH", "3MINDIA", "3PLAND", "3RDROCK", "5PAISA", "63MOONS", "A2ZINFRA", 
    # "AAKASH", "AARON", "AARTIDRUGS", "AARTIIND", "AARVEEDEN", "AARVI", "AAVAS", "ABAN", "ABB", "ABCAPITAL", 
    # "ABFRL", "ABINFRA", "ABMINTLTD", "ABNINT", "ACC", "ACCELYA", "ACCORD", "ACCURACY", "ACE", "ACEINTEG", 
    # "ADANIENT", "ADANIGAS", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "ADANITRANS", "ADFFOODS", "ADHUNIKIND", 
    # "ADLABS", "ADORWELD", "ADROITINFO", "ADSL", "ADVANIHOTR", "ADVENZYMES", "AEGISCHEM", "AFFLE", "AGARIND", 
    # "AGCNET", "AGRITECH", "AGROPHOS", "AHIMSA", "AHLADA", "AHLEAST", "AHLUCONT", "AHLWEST", "AIAENG", "AIONJSW", 
    # "AIRAN", "AIROLAM", "AISL", "AJANTPHARM", "AJMERA", "AJOONI", "AKASH", "AKG", "AKSHARCHEM", "AKSHOPTFBR", 
    # "AKZOINDIA", "ALANKIT", "ALBERTDAVD", "ALCHEM", "ALEMBICLTD", "ALICON", "ALKALI", "ALKEM", "ALKYLAMINE", 
    # "ALLCARGO", "ALLSEC", "ALMONDZ", "ALOKINDS", "ALPA", "ALPHAGEO", "ALPSINDUS", "AMARAJABAT", "AMBANIORG", 
    # "AMBER", "AMBIKCO", "AMBUJACEM", "AMDIND", "AMJLAND", "AMJUMBO", "AMRUTANJAN", "ANANTRAJ", "ANDHRACEMT", 
    # "ANDHRAPAP", "ANDHRSUGAR", "ANGELBRKG", "ANIKINDS", "ANKITMETAL", "ANSALAPI", "ANSALHSG", "ANUP", "APARINDS", 
    # "APCL", "APCOTEXIND", "APEX", "APLAPOLLO", "APLLTD", "APOLLO", "APOLLOHOSP", "APOLLOPIPE", "APOLLOTYRE", 
    # "APOLSINHOT", "APTECHT", "ARCHIDPLY", "ARCHIES", "ARCOTECH", "ARENTERP", "ARIES", "ARIHANT", "ARIHANTSUP", 
    # "ARMANFIN", "AROGRANITE", "ARROWGREEN", "ARSHIYA", "ARSSINFRA", "ARTEDZ", "ARTEMISMED", "ARTNIRMAN", "ARVEE", 
    # "ARVIND", "ARVINDFASN", "ARVSMART", "ASAHIINDIA", "ASAHISONG", "ASAL", "ASALCBR", "ASCOM", "ASHAPURMIN", 
    # "ASHIANA", "ASHIMASYN", "ASHOKA", "ASHOKLEY", "ASIANHOTNR", "ASIANPAINT", "ASIANTILES", "ASIL", "ASLIND", 
    # "ASPINWALL", "ASTEC", "ASTERDM", "ASTRAL", "ASTRAMICRO", "ASTRAZEN", "ASTRON", "ATFL", "ATLANTA", "ATLASCYCLE", 
    # "ATNINTER", "ATUL", "ATULAUTO", "AUBANK", "AURDIS", "Aurionpro", "AUROPHARMA", "AUSOMENT", "AUTOAXLES", "AUTOIND", 
    # "AUTOLITIND", "AVADHSUGAR", "AVANTIFEED", "AVENTUS", "AVG", "AVROIND", "AVSL", "AVTNPL", "AXISBANK", "AXISCADES", 
    # "AYMSYNTEX", "BABAFOOD", "BAFNAPH", "BAGFILMS", "BAJAJ-AUTO", "BAJAJCON", "BAJAJELEC", "BAJAJFINSV", "BAJAJHIND", 
    # "BAJAJHLDNG", "BAJFINANCE", "BALAJITELE", "BALAMINES", "BALAXI", "BALKRISHNA", "BALKRISIND", "BALLARPUR",  
    # "BALMLAWRIE", "BALPHARMA", "BALRAMCHIN", "BANARBEADS", "BANARISUG", "BANCOINDIA", "BANDHANBNK", "BANG", 
    # "BANKA", "BANKBARODA", "BANKINDIA", "BANSWRAS", "BARTRONICS", "BASF", "BASML", "BATAINDIA", "BBL", "BBTC", 
    # "BBTCL", "BCG", "BCONCEPTS", "BDL", "BDR", "BEARDSELL", "BEDMUTHA", "BEL", "BEML", "BEPL", "BERGEPAINT", 
    # "BETA", "BFINVEST", "BFUTILITIE", "BGRENERGY", "BHAGERIA", "BHAGYANGR", "BHAGYAPROP", "BHALCHANDR", 
    # "BHANDARI", "BHARATFORG", "BHARATGEAR", "BHARATRAS", "BHARATWIRE", "BHARTIARTL", "BHEL", "BIGBLOC", 
    # "BIL", "BILENERGY", "BINDALAGRO", "BIOCON", "BIOFILCHEM", "BIRLACABLE", "BIRLACORPN", "BIRLAMONEY", 
    # "BIRLATYRE", "BKMINDST", "BLBLIMITED", "BLISSGVS", "BLKASHYAP", "BLS", "BLUECHIP", "BLUECOAST", "BLUEDART", 
    # "BLUESTARCO", "BODALCHEM", "BOHRA", "BOMDYEING", "BORORENEW", "BOSCHLTD", "BPCL", "BPL", "BRFL", "BRIGADE", 
    # "BRIGHT", "BRITANNIA", "BRNL", "BROOKS", "BSD", "BSE", "BSELINFRA", "BSHSL", "BSL", "BSOFT", "BURNPUR", 
    # "BUTTERFLY", "BVCL", "BYKE", "CADILAHC", "CADSYS", "CALSOFT", "CAMLINFINE", "CAMS", "CANBK", "CANDC", 
    # "CANFINHOME", "CANTABIL", "CAPACITE", "CAPLIPOINT", "CAPTRUST", "CARBORUNIV", "CAREERP", "CARERATING",
    # "CASTEXTECH", "CASTROLIND", "CCCL", "CCHHL", "CCL", 
    # "CDSL", "CEATLTD", "CEBBCO", "CELEBRITY", "CENTENKA", "CENTEXT", 
    # "CENTRALBK", "CENTRUM", "CENTUM", "CENTURYPLY", "CENTURYTEX", "CERA", "CEREBRAINT", "CESC", "CESCVENT", 
    # "CGCL", "CGPOWER", 
    # "CHALET", "CHAMBLFERT", "CHEMBOND", "CHEMCON", "CHEMFAB", "CHENNPETRO", "CHOLAFIN", "CHOLAHLDNG", "CHROMATIC",
    #  "CIGNITITEC", "CIMMCO", "CINELINE", "CINEVISTA", "CIPLA", "CKFSL", "CKPLEISURE", "CKPPRODUCT", "CLEDUCATE", 
    # "CLNINDIA", "CMICABLES", "CMMIPL", "CNOVAPETRO", 
    # "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL", "COMPINFO", "COMPUSOFT", "CONCOR", "CONFIPET", "CONSOFINVT", 
    # "CONTI", "CONTROLPR", "CORALFINAC", "CORDSCABLE", "COROMANDEL", "COSMOFILMS", "COUNCODOS", 
    # "COX&KINGS", "CREATIVE", "CREATIVEYE", "CREDITACC", "CREST", "CRISIL", "CROMPTON", "CROWN", "CSBBANK", "CTE", "CUB", "CUBEXTUB", "CUMMINSIND", "CUPID", "CYBERTECH", "CYIENT", 
    # "DAAWAT", "DABUR", "DALBHARAT", "DALMIASUG", "DAMODARIND", "DANGEE", "DATAMATICS", "DBCORP", "DBL", "DBREALTY", "DBSTOCKBRO", "DCAL", "DCBBANK", "DCI", "DCM", "DCMFINSERV", "DCMNVL", "DCMSHRIRAM", "DCW", "DECCANCE", "DEEPAKFERT", "DEEPAKNTR", "DEEPIND", 
    # "DELTACORP", "DELTAMAGNT", "DEN", "DENORA", "DEVIT", "DFMFOODS", "DGCONTENT", "DHAMPURSUG", "DHANBANK", "DHANUKA", "DHARSUGAR", "DHFL", "DHUNINV", "DIAMONDYD", "DIAPOWER", "DICIND", "DIGISPICE", "DIGJAMLTD", "DISHTV", 
    # "DIVISLAB", "DIXON", "DLF", "DLINKINDIA", "DMART", "DNAMEDIA", "DOLLAR", "DONEAR", "DPABHUSHAN", "DPSCLTD", "DPWIRES", "DQE", "DREDGECORP", "DRL", "DRREDDY", "DRSDILIP", 
    # "DSML", "DSSL", "DTIL", "DUCON", "DVL", "DWARKESH", "DYNAMATECH", "DYNPRO", "E2E", "EASTSILK", "EASUNREYRL", 
    # "EBIXFOREX", "ECLERX", "EDELWEISS", "EDL", "EDUCOMP", "EICHERMOT", "EIDPARRY", "EIFFL", "EIHAHOTELS", "EIHOTEL", 
    # "EIMCOELECO", "EKC", "ELECON", "ELECTCAST", "ELECTHERM", "ELGIEQUIP", "ELGIRUBCO", "EMAMILTD", "EMAMIPAP", "EMAMIREAL", "EMCO", "EMIL", "EMKAY", "EMKAYTOOLS", "EMMBI", "ENDURANCE", "ENERGYDEV", "ENGINERSIN", "ENIL", "EON", "EQUITAS", "EQUITASBNK", 
    # "ERIS", "EROSMEDIA", "ESABINDIA", "ESCORTS", "ESSARSHPNG", "ESSELPACK", "ESTER", "EUROCERA", "EUROMULTI", "EUROTEXIND", "EVEREADY", "EVERESTIND", "EXCEL", "EXCELINDUS", "EXIDEIND", "EXPLEOSOL", "FACT", "FAIRCHEM", 
    # "FCL", "FCONSUMER", "FCSSOFT", "FDC", "FEDERALBNK", "FEL", "FELIX", "FIEMIND", "FILATEX", "FINCABLES", "FINEORG", "FINPIPE", "FLEXITUFF", "FLFL", "FLUOROCHEM", "FMGOETZE", "FMNL", "FOCUS", "FORTIS", "FOSECOIND", "FOURTHDIM", "FRETAIL", "FSC", "FSL", "GABRIEL", 
    # "GAEL", "GAIL", "GAL", "GALAXYSURF", "GALLANTT", "GALLISPAT", "GAMMNINFRA", 
    # "GANDHITUBE", "GANECOS", "GANESHHOUC", "GANGAFORGE", "GANGESSECU", "GARDENSILK", "GARFIBRES", "GATI", "GAYAHWS", "GAYAPROJ", "GBGLOBAL", "GDL", "GEECEE", "GEEKAYWIRE", "GENESYS", "GENUSPAPER", "GENUSPOWER", "GEOJITFSL", 
    # "GEPIL", "GESHIP", "GET&D", "GFLLIMITED", "GFSTEELS", "GHCL", "GICHSGFIN", "GICL", "GICRE", "GILLANDERS", "GILLETTE", "GINNIFILA", "GIPCL", "GIRRESORTS", "GISOLUTION", "GKWLIMITED", "GLAND", "GLAXO", "GLENMARK", "GLFL", "GLOBAL", 
    # "GLOBALVECT", "GLOBE", "GLOBOFFS", "GLOBUSSPR", "GMBREW", "GMDCLTD", "GMMPFAUDLR", "GMRINFRA", "GNA", "GNFC", "GOACARBON", "GOCLCORP", "GODFRYPHLP", "GODHA", "GODREJAGRO", "GODREJCP", "GODREJIND", "GODREJPROP", "GOENKA", "GOKEX", 
    # "GOKUL", "GOKULAGRO", "GOLDENTOBC", "GOLDIAM", "GOLDSTAR", "GOLDTECH", "GOODLUCK", "GPIL", "GPPL", "GPTINFRA",
    # "GRANULES", "GRAPHITE", "GRASIM", "GRAVITA", "GREAVESCOT", "GREENLAM", "GREENPANEL", "GREENPLY", "GREENPOWER", "GRETEX", "GRINDWELL", "GROBTEA", "GRPLTD", "GRSE", "GSCLCEMENT", "GSFC", "GSKCONS", 
    # "GSPL", "GSS", "GTL", "GTLINFRA", "GTNIND", "GTNTEX", "GTPL", "GUFICBIO", "GUJALKALI", "GUJAPOLLO", "GUJGASLTD", "GUJRAFFIA", "GULFOILLUB", "GULFPETRO", "GULPOLY", "GVKPIL", "HAL", "HAPPSTMNDS", "HARITASEAT", "HARRMALAYA", "HATHWAY", "HATSUN", "HAVELLS", "HAVISHA", "HBLPOWER", 
    # "HBSL", "HCC", "HCG", "HCL-INSYS", "HCLTECH", "HDFC", "HDFCAMC", "HDFCBANK", 
    # "HDFCLIFE", "HDIL", "HECPROJECT", "HEG", "HEIDELBERG", "HERCULES", "HERITGFOOD", "HEROMOTOCO", "HESTERBIO", "HEXATRADEX", "HEXAWARE", "HFCL", "HGINFRA", "HGS", "HIKAL", "HIL", "HILTON", "HIMATSEIDE", "HINDALCO", "HINDCOMPOS", "HINDCON", "HINDCOPPER", "HINDMOTORS", 
    # "HINDNATGLS", "HINDOILEXP", "HINDPETRO", "HINDUNILVR", 
    # "HINDZINC", "HIRECT", "HISARMETAL", "HITECH", "HITECHCORP", "HITECHGEAR", "HLVLTD", "HMT", "HMVL", "HONAUT", "HONDAPOWER", "HOTELRUGBY", "HOVS", "HPIL", "HPL", "HSCL", "HSIL", "HTMEDIA", "HUBTOWN", "HUDCO", "HUSYSLTD", "IBREALEST", "IBULHSGFIN", 
    # "IBULISL", "IBVENTURES", "ICEMAKE", "ICICIBANK", "ICICIGI", "ICICIPRULI", "ICIL", "ICRA", "IDBI", "IDEA", "IDFC", 
    # "IDFCFIRSTB", "IEX", "IFBAGRO", "IFBIND", "IFCI", "IFGLEXPOR", "IGARASHI", "IGL", "IGPL", "IIFL", "IIFLSEC", "IIFLWAM", "IITL", "IL&FSENGG", "IL&FSTRANS", "IMFA", "IMPAL", "IMPEXFERRO", "INDBANK", "INDHOTEL", "INDIACEM", "INDIAGLYCO", "INDIAMART", "INDIANB",
    # "INDIANCARD", "INDIANHUME", "INDIGO", "INDLMETER", "INDNIPPON", "INDOCO", "INDORAMA", "INDOSOLAR", "INDOSTAR", "INDOTECH", "INDOTHAI", "INDOWIND", "INDRAMEDCO", "INDSWFTLAB", "INDSWFTLTD", "INDTERRAIN", 
    # "INDUSINDBK", "INEOSSTYRO", "INFIBEAM", "INFOBEAN", "INFOMEDIA", "INFRATEL", "INFY", "INGERRAND", "INNOVANA", "INNOVATIVE", "INOXLEISUR", "INOXWIND", "INSECTICID", "INSPIRISYS", "INTEGRA", "INTELLECT", "INTENTECH", 
    # "INVENTURE", "IOB", "IOC", "IOLCP", "IPCALAB", "IRB", "IRCON", "IRCTC", "IRISDOREME", "ISEC", "ISFT", "ISMTLTD", "ITC", "ITDC", "ITDCEM", "ITI", "IVC", "IVP", "IZMO", "J&KBANK", "JAGRAN", "JAGSNPHARM", "JAIBALAJI", "JAICORPLTD", "JAIHINDPRO", "JAINSTUDIO", "JAKHARIA", "JALAN", 
    # "JAMNAAUTO", "JASH", "JAYAGROGN", "JAYBARMARU", "JAYNECOIND", "JAYSREETEA", "JBCHEPHARM", "JBFIND", "JBMA", "JCHAC", "JETAIRWAYS", "JETFREIGHT", "JETKNIT", 
    # "JHS", "JIKIND", "JINDALPHOT", "JINDALPOLY", "JINDALSAW", "JINDALSTEL", "JINDRILL", "JINDWORLD", "JISLJALEQS", "JITFINFRA", "JKCEMENT", "JKIL", "JKLAKSHMI", "JKPAPER", "JKTYRE", "JMA", "JMCPROJECT", "JMFINANCIL", "JMTAUTOLTD", "JOCIL", "JPASSOCIAT", "JPINFRATEC", 
    # "JPOLYINVST", "JPPOWER", "JSL", "JSLHISAR", "JSWENERGY", "JSWHL", "JSWSTEEL", "JTEKTINDIA", "JUBILANT", "JUBLFOOD", "JUBLINDS", "JUSTDIAL", "JVLAGRO", "JYOTHYLAB", "JYOTISTRUC", "KABRAEXTRU", "KAJARIACER", "KAKATCEM", "KALPATPOWR", "KALYANI", "KALYANIFRG", 
    # "KAMATHOTEL", "KAMDHENU", "KANANIIND", "KANORICHEM", "KANSAINER", "KAPSTON", "KARDA", "KARMAENG", "KARURVYSYA", "KAUSHALYA", "KAYA", "KCP", "KCPSUGIND", "KDDL", "KEC", "KECL", "KEERTI", "KEI", "KELLTONTEC", "KERNEX", "KESORAMIND", "KEYFINSERV", "KGL", "KHADIM", "KHANDSE", 
    # "KHFM", "KICL", "KILITCH", "KINGFA", "KIOCL", "KIRIINDUS", "KIRLOSBROS", "KIRLOSENG", 
    # "KIRLOSIND", "KITEX", "KKCL", "KKVAPOW", "KMSUGAR", "KNRCON", "KOHINOOR", "KOKUYOCMLN", "KOLTEPATIL", 
    # "KOPRAN", "KOTAKBANK", "KOTARISUG", "KOTHARIPET", "KOTHARIPRO", "KPITTECH", "KPRMILL", "KRBL", "KREBSBIO", 
    # "KRIDHANINF", "KRISHANA", "KRITIKA", "KSB", "KSCL", "KSERASERA", "KSHITIJPOL", "KSK", "KSL", "KTKBANK",
    # "KUANTUM", "KWALITY", "L&TFH", "LAGNAM", "LAKPRE", "LAKSHVILAS", "LALPATHLAB", "LAMBODHARA", "LAOPALA", 
    # "LASA", "LATTEYS", "LAURUSLABS", "LAXMICOT", "LAXMIMACH", "LEMONTREE", "LEXUS", "LFIC", "LGBBROSLTD", 
    # "LGBFORGE", "LIBAS", "LIBERTSHOE", "LICHSGFIN", "LIKHITHA", "LINCOLN", "LINCPEN", "LINDEINDIA", "LOKESHMACH",
    # "LOTUSEYE", "LOVABLE", "LPDC", "LSIL", "LT", "LTI", "LTTS", "LUMAXIND", "LUMAXTECH", "LUPIN", "LUXIND", 
    # "LYKALABS", "LYPSAGEMS", "M&M", "M&MFIN", "MAANALU", "MACPOWER", "MADHAV", "MADHUCON", "MADRASFERT", 
    # "MAGADSUGAR", "MAGMA", "MAGNUM", "MAHABANK", "MAHAPEXLTD", "MAHASTEEL", "MAHEPC", "MAHESHWARI", "MAHICKRA", "MAHINDCIE", "MAHLIFE", "MAHLOG", "MAHSCOOTER", "MAHSEAMLES", "MAITHANALL", "MAJESCO", "MALUPAPER", "MANAKALUCO", "MANAKCOAT", "MANAKSIA", "MANAKSTEEL", "MANALIPETC", "MANAPPURAM", "MANAV", "MANGALAM", "MANGCHEFER", "MANGLMCEM", "MANGTIMBER", "MANINDS", "MANINFRA", "MANUGRAPH", "MARALOVER", "MARATHON", "MARICO", "MARINE", "MARKSANS", "MARSHALL", 
    # "MARUTI", "MASFIN", "MASKINVEST", "MASTEK", "MATRIMONY", "MAWANASUG", "MAXHEALTH", "MAXINDIA", "MAXVIL", "MAYURUNIQ", "MAZDA", "MAZDOCK", "MBAPL", "MBECL", "MBLINFRA", "MCDHOLDING", "MCDOWELL-N", "MCL", "MCLEODRUSS", "MDL", "MEGASOFT", "MEGH", "MELSTAR", "MENONBE", "MEP", "MERCATOR", "METALFORGE", "METKORE", "METROPOLIS", "MFSL", "MGEL", "MGL", "MHHL", "MHRIL", "MIC", "MIDHANI", "MILTON", "MINDACORP", "MINDAIND", "MINDPOOL", "MINDTECK", "MINDTREE", "MIRCELECTR", "MIRZAINT", "MITCON", "MITTAL", "MKPL", "MMFL", "MMNL", "MMP", "MMTC", "MODIRUBBER", "MOHITIND", "MOHOTAIND", "MOIL", "MOKSH", "MOLDTECH", "MOLDTKPAC", "MONTECARLO", "MORARJEE", "MOREPENLAB", "MOTHERSUMI", "MOTILALOFS", "MOTOGENFIN", "MPHASIS", "MPSLTD", "MPTODAY", "MRF", "MRO", "MRO-TEK", "MRPL", "MSPL", "MSTCLTD", "MTEDUCARE", "MTNL", "MUKANDENGG", "MUKANDLTD", 
    # "MUKTAARTS", "MUNJALAU", "MUNJALSHOW", "MURUDCERA", "MUTHOOTCAP", "MUTHOOTFIN", "NACLIND", "NAGAFERT", "NAGREEKCAP", "NAGREEKEXP", "NAHARCAP", "NAHARINDUS", "NAHARPOLY", "NAHARSPING", "NAM-INDIA", "NANDANI", "NARMADA", "NATCOPHARM", "NATHBIOGEN", "NATIONALUM", "NATNLSTEEL", "NAUKRI", "NAVINFLUOR", "NAVKARCORP", "NAVNETEDUL", "NBCC", "NBIFIN", "NBVENTURES", "NCC", "NCLIND", "NDGL", "NDL", "NDTV", "NECCLTD", "NECLIFE", "NELCAST", "NELCO", "NEOGEN", "NESCO", "NETWORK18", "NEULANDLAB", "NEWGEN", "NEXTMEDIA", "NFL", "NH", "NHPC", "NIACL", "NIBL", "NIITLTD", "NILAINFRA", "NILASPACES", "NILKAMAL", "NIPPOBATRY", "NIRAJISPAT", "NITCO", "NITINFIRE", "NITINSPIN", "NITIRAJ", "NKIND", "NLCINDIA", "NMDC", "NOCIL", "NOIDATOLL", "NORBTEAEXP", "NRAIL", "NRBBEARING", "NSIL", "NTL", "NTPC", "NUCLEUS", "NXTDIGITAL", "OAL", "OBEROIRLTY", "OCCL", "OFSS", "OIL", "OILCOUNTUB", "OISL", "OLECTRA", "OMAXAUTO", "OMAXE", "OMFURN", 
    # "OMKARCHEM", "OMMETALS", "ONELIFECAP", "ONEPOINT", "ONGC", "ONMOBILE", "ONWARDTEC", "OPAL", "OPTIEMUS", "OPTOCIRCUI", "ORBTEXP", "ORCHIDPHAR", "ORICONENT", "ORIENTABRA", "ORIENTALTL", "ORIENTBELL", "ORIENTCEM", "ORIENTELEC", "ORIENTHOT", "ORIENTLTD", "ORIENTPPR", "ORIENTREF", "ORISSAMINE", "ORTEL", "ORTINLABSS", "OSIAHYPER", "OSWALAGRO", "OSWALSEEDS", "PAEL", "PAGEIND", "PAISALO", "PALASHSECU", "PALREDTEC", "PANACEABIO", "PANACHE", "PANAMAPET", "PANSARI", "PAPERPROD", "PAR", "PARABDRUGS", "PARACABLES", "PARAGMILK", "PARIN", "PARSVNATH", "PASHUPATI", "PATELENG", "PATINTLOG", "PATSPINLTD", "PCJEWELLER", "PDMJEPAPER", "PDSMFL", "PEARLPOLY", "PEL", "PENIND", "PENINLAND", "PENTAGOLD", "PERFECT", "PERSISTENT", "PETRONET", "PFC", "PFIZER", "PFOCUS", "PFS", "PGEL", "PGHH", "PGHL", "PGIL", "PHILIPCARB", 
    # "PHOENIXLTD", "PIDILITIND", "PIGL", "PIIND", "PILANIINVS", "PILITA", "PIONDIST", "PIONEEREMB", "PITTIENG", "PKTEA", "PLASTIBLEN", "PNB", "PNBGILTS", "PNBHOUSING", "PNC", "PNCINFRA", "PODDARHOUS", "PODDARMENT", "POKARNA", "POLYCAB", "POLYMED", "POLYPLEX", "PONNIERODE", "POWERFUL", "POWERGRID", "POWERINDIA", "POWERMECH", "PPAP", "PPL", "PRABHAT", "PRADIP", "PRAENG", "PRAJIND", "PRAKASH", "PRAKASHSTL", "PRAXIS", "PRECAM", "PRECOT", "PRECWIRE", "PREMEXPLN", "PREMIER", "PREMIERPOL", "PRESSMN", "PRESTIGE", "PRICOLLTD", "PRIMESECU", "PRINCEPIPE", "PRITI", "PROLIFE", "PROSEED", "PROZONINTU", "PRSMJOHNSN", "PSB", "PSL", "PSPPROJECT", "PTC", "PTL", "PULZ", "PUNJABCHEM", "PUNJLLOYD", "PURVA", "PUSHPREALM", "PVR", "QUESS", "QUICKHEAL", "RADAAN", "RADICO", "RADIOCITY", "RAIN", "RAJESHEXPO", "RAJMET", "RAJOIL", "RAJRAYON", 
    # "RAJSREESUG", "RAJTV", "RALLIS", "RAMANEWS", "RAMASTEEL", "RAMCOCEM", "RAMCOIND", "RAMCOSYS", "RAMKY", "RAMSARUP", "RANASUG", "RANEENGINE", "RANEHOLDIN", "RATNAMANI", "RAYMOND", "RBL", "RBLBANK", "RCF", "RCOM", "RECLTD", "REDINGTON", "REFEX", "RELAXO", "RELCAPITAL", "RELIABLE", "RELIANCE", "RELIGARE", "RELINFRA", "REMSONSIND", "RENUKA", "REPCOHOME", "REPL", "REPRO", "RESPONIND", "REVATHI", "RGL", "RHFL", "RICOAUTO", "RIIL", "RITES", "RKDL", "RKEC", "RKFORGE", "RMCL", "RMDRIP", "RML", "RNAVAL", "ROHITFERRO", "ROHLTD", "ROLLT", "ROLTA", "ROSSARI", "ROSSELLIND", "ROUTE", "RPGLIFE", "RPOWER", "RPPINFRA", "RPPL", "RSSOFTWARE", "RSWM", "RSYSTEMS", "RTNINFRA", "RTNPOWER", "RUBYMILLS", "RUCHI", "RUCHINFRA", "RUCHIRA", "RUPA", "RUSHIL", "RVNL", "S&SPOWER", "SABEVENTS", "SABTN", "SADBHAV", "SADBHIN", "SAFARI", "SAGARDEEP", 
    # "SAGCEM", "SAIL", "SAKAR", "SAKETH", "SAKHTISUG", "SAKSOFT", "SAKUMA", "SALASAR", "SALONA", "SALSTEEL", "SALZERELEC", "SAMBHAAV", "SANCO", "SANDESH", "SANDHAR", "SANGAMIND", "SANGHIIND", "SANGHVIFOR", "SANGHVIMOV", "SANGINITA", "SANOFI", "SANWARIA", "SARDAEN", "SAREGAMA", "SARLAPOLY", "SARVESHWAR", "SASKEN", "SASTASUNDR", "SATHAISPAT", "SATIA", "SATIN", "SBICARD", "SBILIFE", "SBIN", "SCHAEFFLER", "SCHAND", "SCHNEIDER", "SCI", "SDBL", "SEAMECLTD", "SECL", "SECURCRED", "SELAN", "SELMCL", "SEPOWER", "SEQUENT", "SERVOTECH", "SESHAPAPER", "SETCO", "SETUINFRA", "SEYAIND", "SEZAL", "SFL", "SGL", "SHAHALLOYS", "SHAIVAL", "SHAKTIPUMP", "SHALBY", "SHALPAINTS", "SHANKARA", "SHANTI", "SHANTIGEAR", "SHARDACROP", "SHARDAMOTR", "SHARONBIO", "SHEMAROO", "SHIL", "SHILPAMED", "SHIRPUR-G", "SHIVAMAUTO", "SHIVAMILLS", "SHIVATEX", "SHIVAUM", "SHK", "SHOPERSTOP", "SHRADHA", "SHREDIGCEM", 
    # "SHREECEM", "SHREEPUSHK", "SHREERAMA", "SHRENIK", "SHREYANIND", "SHREYAS", "SHRIPISTON", "SHRIRAMCIT", "SHRIRAMEPC", "SHUBHLAXMI", "SHYAMCENT", "SHYAMMETL", "SHYAMTEL", "SICAGEN", "SICAL", "SIEMENS", "SIGIND", "SIKKO", "SIL", "SILGO", "SILINV", "SILLYMONKS", "SILVERTUC", "SIMBHALS", "SIMPLEXINF", "SINTERCOM", "SINTEX", "SIRCA", "SIS", "SITINET", "SIYSIL", "SJVN", "SKFINDIA", "SKIL", "SKIPPER", "SKMEGGPROD", "SKSTEXTILE", "SMARTLINK", "SMLISUZU", "SMPL", "SMSLIFE", "SMSPHARMA", "SMVD",
    # "SNOWMAN", "SOBHA", "SOFTTECH", "SOLARA", "SOLARINDS", "SOLEX", "SOMANYCERA", "SOMATEX", "SOMICONVEY", "SONAHISONA", "SONAMCLOCK", "SONATSOFTW", "SONISOYA", "SORILINFRA", "SOTL", "SOUTHBANK", "SOUTHWEST", "SPAL", "SPANDANA", "SPARC", "SPCENET", "SPECIALITY", "SPECTRUM", "SPENCERS", "SPENTEX", "SPIC", "SPLIL", "SPMLINFRA", "SPTL", "SPYL", "SREEL", "SREINFRA", "SRF", "SRHHYPOLTD", "SRIPIPES", "SRIRAM", "SRPL", "SRTRANSFIN", "SSINFRA", "SSWL", "STAMPEDE", "STAR", "STARCEMENT", "STARPAPER", 
    # "STCINDIA", "STEELCITY", "STEELXIND", "STEL", "STERTOOLS", "STINDIA", "STRTECH", "SUBCAPCITY", "SUBEXLTD", "SUBROS", "SUDARSCHEM", "SUJANAUNI", "SUMEETINDS", "SUMICHEM", "SUMIT", "SUMMITSEC", "SUNCLAYLTD", "SUNDARAM", "SUNDARMFIN", "SUNDARMHLD", "SUNDRMBRAK", "SUNDRMFAST", "SUNFLAG", "SUNPHARMA", "SUNTECK", "SUNTV", "SUPERHOUSE", "SUPERSPIN", "SUPPETRO", "SUPRAJIT", "SUPREMEENG", "SUPREMEIND", "SUPREMEINF", "SURANASOL", "SURANAT&P", "SURANI", "SUREVIN", "SURYALAXMI", "SURYAROSNI", "SUTLEJTEX", "SUULD", "SUVEN", "SUVENPHAR", "SUZLON", "SVLL", "SWANENERGY", "SWARAJENG", "SWELECTES", "SWSOLAR", "SYMPHONY", "SYNCOM", "SYNGENE", "TAINWALCHM", "TAJGVK", "TAKE", "TALBROAUTO", "TALWALKARS", "TALWGYM", "TANLA", "TANTIACONS", "TARACHAND", "TARMAT", "TASTYBITE", "TATACHEM", "TATACOFFEE", "TATACOMM", "TATACONSUM", "TATAELXSI", 
    # "TATAINVEST", "TATAMETALI", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TATASTLBSL", "TATASTLLP", "TBZ", "TCI", "TCIDEVELOP", "TCIEXP", "TCIFINANCE", "TCNSBRANDS", "TCPLPACK", "TCS", "TDPOWERSYS", "TEAMLEASE", "TECHIN", "TECHM", "TECHNOE", "TECHNOFAB", "TEJASNET", "TERASOFT", "TEXINFRA", "TEXMOPIPES", "TEXRAIL", "TFCILTD", "TFL", "TGBHOTELS", "THANGAMAYL", "THEINVEST", "THEJO", "THEMISMED", "THERMAX", "THIRUSUGAR", "THOMASCOOK", "THOMASCOTT", "THYROCARE", "TI", "TIDEWATER", "TIIL", "TIINDIA", "TIJARIA", "TIL", "TIMESGTY", "TIMETECHNO", "TIMKEN", "TINPLATE", "TIPSINDLTD", "TIRUMALCHM", "TIRUPATI", "TIRUPATIFL", "TITAN", "TMRVL", "TNPETRO", "TNPL", "TNTELE", "TOKYOPLAST", "TORNTPHARM", "TORNTPOWER", "TOTAL", "TOUCHWOOD", "TPLPLASTEH", "TRANSWIND", "TREEHOUSE", "TREJHARA", "TRENT", "TRF", "TRIDENT", "TRIGYN", "TRIL", "TRITURBINE", "TRIVENI", 
    # "TTKHLTCARE", "TTKPRESTIG", "TTL", "TTML", "TV18BRDCST", "TVSELECT", "TVSMOTOR", "TVSSRICHAK", "TVTODAY", "TVVISION", "TWL", "UBL", "UCALFUEL", "UCL", "UCOBANK", "UFLEX", "UFO", "UGARSUGAR", "UGROCAP", "UJAAS", "UJJIVAN", "UJJIVANSFB", "ULTRACEMCO", "UMANGDAIRY", "UMESLTD", "UNICHEMLAB", "UNIENTER", "UNIINFO", "UNIONBANK", "UNIPLY", "UNITECH", "UNITEDPOLY", "UNITEDTEA", "UNITY", "UNIVASTU", "UNIVCABLES", "UNIVPHOTO", "UPL", "URAVI", "URJA", "USHAMART", 
    # "UTIAMC", "UTTAMSTL", "UTTAMSUGAR", "UVSL", "UWCSL", "V2RETAIL", "VADILALIND", "VAIBHAVGBL", "VAISHALI", "VAKRANGEE", "VARDHACRLC", "VARDMNPOLY", "VARROC", "VASA", "VASCONEQ", "VASWANI", "VBL", "VCL", "VEDL", "VENKEYS", "VENUSREM", "VERA", "VERTOZ", "VESUVIUS", "VETO", "VGUARD", "VHL", "VICEROY", "VIDEOIND", "VIDHIING", "VIJIFIN", "VIKASECO", "VIKASMCORP", "VIMTALABS", "VINATIORGA", "VINDHYATEL", "VINNY", "VINYLINDIA", "VIPCLOTHNG", "VIPIND", "VIPULLTD", "VISAKAIND", "VISASTEEL", "VISHNU", 
    # "VISHWARAJ", "VIVIDHA", "VIVIMEDLAB", "VLSFINANCE", "VMART", "VOLTAMP", "VOLTAS", "VRLLOG", "VSCL", "VSSL", 
    # "VSTIND", "VSTTILLERS", "VTL", "WABAG", "WABCOINDIA", "WALCHANNAG", "WANBURY", "WEALTH", "WEBELSOLAR", 
    # "WEIZMANIND", "WELCORP", "WELENT", "WELINV", "WELSPUNIND", "WENDT", "WFL", "WHEELS", "WHIRLPOOL", "WILLAMAGOR",
    # "WINDMACHIN", "WIPL", "WIPRO", "WOCKPHARMA", "WONDERLA", "WORTH", "WSI", "WSTCSTPAPR", "XCHANGING", 
    "XELPMOC", "XPROINDIA", "YESBANK", "ZAGGLE", "ZEEL", "ZEELEARN", "ZEEMEDIA", "ZENITHBIR", "ZENITHEXPO", 
    "ZENSARTECH", "ZENTEC", "ZICOM", "ZODIAC", "ZODIACLOTH", "ZODJRDMKJ", "ZOTA", "ZUARI", "ZUARIGLOB", "ZYDUSWELL"
  ]



def getStartdate(month, year):
    # Month ki first date calculate karna
    first_day = 1
    # Date ko dd/mm/yyyy format mein convert karna
    start_date = f"{first_day:02d}/{month:02d}/{year}"
    return start_date

def getEnddate(month, year):
    # Month ki last date calculate karna
    last_day = calendar.monthrange(year, month)[1]
    # Date ko dd/mm/yyyy format mein convert karna
    end_date = f"{last_day:02d}/{month:02d}/{year}"
    return end_date

reset = False
startYear = 2020
endYear = 2022
startMonth = 1
endMonth = 13
def build_Scriptrangetodownload(scripts):
    mydb = open_connection()
    mycursor = mydb.cursor()
    if(toReset):
        mycursor.execute("delete from script_range_to_download where workstatus!='COMPLETED' ")
        mydb.commit()
    for script in scripts:
        print("processing for "+script)
        for y in range(startYear, endYear):
            for m in range(startMonth, endMonth):
                start_date = getStartdate(m, y)
                end_date = getEnddate(m, y)
                # print(startdate)
                # print(enddate)
                sqlQry = "insert into script_range_to_download values(NULL, '"+script+"', "+str(m)+", "+str(y)+", '"+start_date+"','"+end_date+"',  'TODOWNLOAD')"
                try:
                    mycursor.execute(sqlQry)
                except  Exception as e:
                    print("Err in "+sqlQry, e )
    mydb.commit()
    mydb.close()

def get_ScriptDataToFatch(scriptName):
    mydb = open_connection()    
    mycursor = mydb.cursor()
    mycursor.execute("SELECT scriptname, year, month from script_range_to_download where workstatus='TODOWNLOAD' and scriptName='"+scriptName+"' order by scriptname DESC, year DESC, month DESC limit 1;")
    firstRow = mycursor.fetchone()
    mydb.close()
    return firstRow

def build_scriptTradeData(dataframe, scriptName):
    # update_work_status(scriptName, "INPROGRESS") 
    dataframe.index.name = 'idno'
    dataframe.index = np.arange(1, len(dataframe) + 1)
    dataframe.index.name = 'idno'
    # engine = create_engine("mysql+mysqlconnector://root:Root@localhost/stockmarket")
    # connection = engine.connect()
    print(dataframe)
    table_name = 'trade_record_'+(scriptName.lower().replace(" ","_"))
    create_table_insert_data(dataframe,table_name)

def create_table_insert_data(dataframe,table_name,):
    mydb = open_connection()
    cursor = mydb.cursor()
    query = f"SHOW TABLES LIKE '{table_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    if not result:
       # Table doesn't exist, create it
        create_table_query = f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                `script` VARCHAR(255),
                `stock` VARCHAR(255),
                `high` FLOAT,
                `low` FLOAT,
                `open` FLOAT,
                `close` FLOAT,
                `last_price` FLOAT,
                `prev_close` FLOAT,
                `quantity` INT,
                `traded_value` FLOAT,
                `52W_high` FLOAT,
                `52 Week Low Price` FLOAT,
                `tradetime2` DATETIME
            )
        """
        cursor.execute(create_table_query)
        print("Table created!") 
    # Insert data into the table
    for index, row in dataframe.iterrows():
        insert_query = f"INSERT INTO {table_name} (`script`, `stock`, `high`, `low`, `open`, `close`, `last_price`, `prev_close`, `quantity`, `traded_value`, `52W_high`, `52 Week Low Price`, `tradetime2`) VALUES ('{row['script']}','{row['stock']}','{row['high']}','{row['low']}','{row['open']}','{row['close']}','{row['last_price']}','{row['prev_close']}','{row['quantity']}','{row['traded_value']}','{row['52W_high']}','{row['52 Week Low Price']}',NULL)"
        print(insert_query)
        print("Table Already exists!")
        cursor.execute(insert_query)

    # Commit changes to the database
    mydb.commit()  
    print(f"New table '{table_name}' created and data inserted.")

def get_all_pending_script_names(mydb):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT distinct scriptname FROM script_range_to_download  where workstatus='TODOWNLOAD' order by scriptname")
    allPendingScriptNames = mycursor.fetchall()
    return allPendingScriptNames
     

def process_to_download_and_persist(scriptName, semaphore):
    print("thread running for script:"+scriptName)
    dataToFetch = get_ScriptDataToFatch(scriptName)
    print(dataToFetch)
    startDate = "01-"+str(dataToFetch[2])+"-"+str(dataToFetch[1])    
    if(dataToFetch[2]==12):
        endDate = "01-1-"+str(dataToFetch[1]+1)
    else:
        endDate = "01-"+str(dataToFetch[2]+1)+"-"+str(dataToFetch[1])
    print("downloading and uploading for "+dataToFetch[0] +" <"+startDate+", "+endDate+">")
    dataframeRate = stocks.get_data(stock_symbol=scriptName, full_data=False, start_date=startDate, end_date=endDate)
    build_scriptTradeData(dataframeRate, scriptName)

# def main():
#     scriptNames = get_all_pending_script_names()
#     if scriptNames== None:
#         build_Scriptrangetodownload(all_scripts)
#     thread_count = 5  # Number of threads

#     for scriptname in scriptNames:
#         process_to_download_and_persist(scriptname[0])


# -----------------------------------------thread new code------------------------------------------
def main():
    mydb = open_connection()
    scriptNames = get_all_pending_script_names(mydb)
    print(len(scriptNames))
    if (scriptNames== None) | (len(scriptNames)==0) :
        build_Scriptrangetodownload(all_scripts)
    max_allow_thread = 4  # Number of threads
    semaphore = threading.Semaphore(max_allow_thread)

    threads=[]
    for scriptname in scriptNames:
        thread = threading.Thread(target=process_to_download_and_persist, args=(scriptname[0], semaphore))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print("Threads Completed!")    
        

main()
# ---------------------New Code -----------------------------------------------------------------------

# Assuming you have imported the necessary libraries and defined the required functions

# def update_work_status(script_name, newStatus):
#     mycursor = mydb.cursor()
#     update_query = f"UPDATE script_range_to_download SET workstatus='"+newStatus+"' WHERE scriptname='{script_name}'"
#     mycursor.execute(update_query)
#     mydb.commit()
#     print(f"Work status updated to 'COMPLETED' for script: {script_name}")


# def worker(q):
#     while True:
#         item = q.get()
#         if item is None:
#             break
#         scriptName, startDate, endDate = item
#         print("Downloading and uploading for " + scriptName + " <" + startDate + ", " + endDate + ">")
        # dataframeRate = stocks.get_data(stock_symbol=scriptName, full_data=False, start_date=startDate, end_date=endDate)
#         build_scriptTradeData(dataframeRate, scriptName)
#         update_work_status(scriptName, "COMPLETED")  # Updating work status here
#         q.task_done()


# def main():
#     build_Scriptrangetodownload()
#     thread_count = 5  # Number of threads
#     q = queue.Queue()
#     threads = []
    
#     for _ in range(thread_count):
#         t = threading.Thread(target=worker, args=(q,))
#         t.start()
#         threads.append(t)

#     isRecordAvailable = True
#     while isRecordAvailable:
#         dataToFetch = get_ScriptDataToFatch()
            
#         if dataToFetch is None:
#             isRecordAvailable = False
#             break
#         scriptName = dataToFetch[0]
#         startDate = "01-" + str(dataToFetch[2]) + "-" + str(dataToFetch[1])
#         if dataToFetch[2] == 12:
#             endDate = "01-1-" + str(dataToFetch[1] + 1)
#         else:
#             endDate = "01-" + str(dataToFetch[2] + 1) + "-" + str(dataToFetch[1])
#         q.put((scriptName, startDate, endDate))
#         # update_selected_rows_status(dataTofetch, "started")  #function banana h jo data ki progree change karde(work_status) 
    

#     q.join()
#     for _ in range(thread_count):
#         q.put(None)

#     for t in threads:
#         t.join()


#     # mydb.close()
#     # cursor.close()
#     print("All tasks completed.")

# if __name__ == "__main__":
#     main()
