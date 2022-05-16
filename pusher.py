import mysql.connector
from mysql.connector import Error
import csv
import glob
import os
#Text validation
import enchant

#Location resolver
from geopy.geocoders import Nominatim
import time
from pprint import pprint

#Reverse geocoding
import reverse_geocoder  as rg

keyword_stop_words = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

skip_until = 457

countryList = [
	"afghanistan",
	"albania",
	"algeria",
	"americansamoa",
	"andorra",
	"angola",
	"anguilla",
	"antarctica",
	"antiguaandbarbuda",
	"argentina",
	"armenia",
	"aruba",
	"australia",
	"austria",
	"azerbaijan",
	"bahamas(the)",
	"bahrain",
	"bangladesh",
	"barbados",
	"belarus",
	"belgium",
	"belize",
	"benin",
	"bermuda",
	"bhutan",
	"bolivia",
	"bonaire",
	"bosniaandherzegovina",
	"botswana",
	"bouvetisland",
	"brazil",
    "brasil",
	"british",
	"britan",
	"bruneidarussalam",
	"bulgaria",
	"burkina faso",
	"burundi",
	"caboverde",
	"cambodia",
	"cameroon",
	"canada",
	"caymanislands",
	"centralafricanrepublic",
	"chad",
	"chile",
	"china",
	"christmasisland",
	"cocosislands",
	"keeling",
	"colombia",
	"comoros",
	"congo",
	"congo",
	"cook islands",
	"costa rica",
	"croatia",
	"cuba",
	"curaçao",
	"cyprus",
	"czechia",
	"côte d'ivoire",
	"denmark",
	"djibouti",
	"dominica",
	"dominicanrepublic",
	"ecuador",
	"egypt",
	"el salvador",
	"equatorial guinea",
	"eritrea",
	"estonia",
	"eswatini",
	"ethiopia",
	"falkland islands",
	"faroe islands",
	"fiji",
	"finland",
	"france",
	"frenchguiana",
	"frenchpolynesia",
	"frenchsouthernterritories",
	"gabon",
	"gambia",
	"georgia",
	"germany",
	"ghana",
	"gibraltar",
	"greece",
	"greenland",
	"grenada",
	"guadeloupe",
	"guam",
	"guatemala",
	"guernsey",
	"guinea",
	"guinea-bissau",
	"guyana",
	"haiti",
	"heard island and mcdonald islands",
	"holy see",
	"honduras",
	"hong kong",
	"hungary",
	"iceland",
	"india",
	"indonesia",
	"iran",
	"iraq",
	"ireland",
	"isle of man",
	"israel",
	"italy",
	"jamaica",
	"japan",
	"jersey",
	"jordan",
	"kazakhstan",
	"kenya",
	"kiribati",
	"korea",
	"korea",
	"north korea",
	"south korea",
	"kuwait",
	"kyrgyzstan",
	"lao people's democratic republic",
	"latvia",
	"lebanon",
	"lesotho",
	"liberia",
	"libya",
	"liechtenstein",
	"lithuania",
	"luxembourg",
	"macao",
	"madagascar",
	"malawi",
	"malaysia",
	"maldives",
	"mali",
	"malta",
	"marshall islands",
	"martinique",
	"mauritania",
	"mauritius",
	"mayotte",
	"mexico",
	"micronesia",
	"moldova",
	"monaco",
	"mongolia",
	"montenegro",
	"montserrat",
	"morocco",
	"mozambique",
	"myanmar",
	"namibia",
	"nauru",
	"nepal",
	"netherlands",
	"new caledonia",
	"new zealand",
	"nicaragua",
	"niger",
	"nigeria",
	"niue",
	"norfolk island",
	"northern mariana islands",
	"norway",
	"oman",
	"pakistan",
	"palau",
	"palestine, state of",
	"panama",
	"papua new guinea",
	"paraguay",
	"peru",
	"philippines",
	"pitcairn",
	"poland",
	"portugal",
	"puerto rico",
	"qatar",
	"republic of north macedonia",
	"romania",
	"russian federation",
	"rwanda",
	"réunion",
	"saint barthélemy",
	"saint helena, ascension and tristan da cunha",
	"saint kitts and nevis",
	"saint lucia",
	"saint martin",
	"saint pierre and miquelon",
	"saint vincent and the grenadines",
	"samoa",
	"san marino",
	"sao tome and principe",
	"saudi arabia",
	"senegal",
	"serbia",
	"seychelles",
	"sierra leone",
	"singapore",
	"sint maarten",
	"slovakia",
	"slovenia",
	"solomon islands",
	"somalia",
	"southafrica",
	"southgeorgia and the south sandwich islands",
	"southsudan",
	"spain",
	"srilanka",
	"sudan",
	"suriname",
	"svalbard and jan mayen",
	"sweden",
	"switzerland",
	"syrian arab republic",
	"taiwan",
	"tajikistan",
	"tanzania, united republic of",
	"thailand",
	"timor-leste",
	"togo",
	"tokelau",
	"tonga",
	"trinidad and tobago",
	"tunisia",
	"turkey",
	"turkmenistan",
	"turks and caicos islands",
	"tuvalu",
	"uganda",
	"ukraine",
	"unitedarab emirates",
	"unitedkingdom",
	"unitedstatesminoroutlyingislands",
	"unitedstates",
	"uruguay",
	"uzbekistan",
	"vanuatu",
	"venezuela",
	"vietnam",
	"virginislands",
	"virginislands ",
	"wallisandfutuna",
	"westernsahara",
	"yemen",
	"zambia",
	"zimbabwe",
	"ålandislands"]

state_names = ["alaska", "alabama", "arkansas", "american samoa", "arizona", "california", "colorado", "connecticut", "district ", "of columbia", "delaware", "florida", "georgia", "guam", "hawaii", "iowa", "idaho", "illinois", "indiana", "kansas", "kentucky", "louisiana", "massachusetts", "maryland", "maine", "michigan", "minnesota", "missouri", "mississippi", "montana", "north carolina", "north dakota", "nebraska", "new hampshire", "new jersey", "new mexico", "nevada", "new york", "ohio", "oklahoma", "oregon", "pennsylvania", "puerto rico", "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah", "virginia", "virgin islands", "vermont", "washington", "wisconsin", "west virginia", "wyoming"]

app = Nominatim(user_agent="resolver")
def locationResolver(location_to_resolve):
    try:
        location = app.geocode(location_to_resolve)
        if(location != None):
            lat = location.raw["lat"]
            lon = location.raw["lon"]
            coordinates = (lat, lon)
            
            display_name = location.raw["display_name"]

            display_name_split = display_name.split(",")
            for i in display_name_split:
                itmp = i.lower()
                itmp = itmp.replace(" ","")
                if itmp in countryList:
                    return (itmp, coordinates)
        else:
            return False
    except:
        return False
        
        

    return False




try:
    connection = mysql.connector.connect(host='csmysql.cs.cf.ac.uk',
                                         database='c2064607_CM2305_Group_Project',
                                         user='c2064607',
                                         password='Groupwork123')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
    
path = os.getcwd()
csv_files = glob.glob(os.path.join(path, "*.csv"))

ID_definer = 0
for f in csv_files:
    with open(f, encoding='utf-8', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
                

        linectr = -1
        
        for i in spamreader: #For entry
            ID_definer += 1
            linectr += 1
            if(ID_definer < skip_until):
                continue
            if(True):
                database_entry = []
                indx = 0
                for i2 in i:
                    if(indx == 0): #Date
                        database_entry.append(i2)
                        pass
                    if(indx == 1): #hashtags
                        hshtags_final = []
                        i2split = i2.split(", ")
                        for hshtag in i2split:
                            hshtag = hshtag.replace("[","")
                            hshtag = hshtag.replace("\'","")
                            hshtag = hshtag.replace("]'","")
                            #print(hshtag)
                            hshtags_final.append(hshtag)
                        database_entry.append(hshtags_final)
                            
                    if(indx == 2): #tweet ids
                        i2 = i2.replace("[","")
                        i2 = i2.replace("]","")
                        strx = i2.split("), (")
                        tweet_ids_final = []
                        for twt_id in strx:
                            strx2 = twt_id.split(", ")
                            strx3 = strx2[0]
                            strx3 = strx3.replace("(","")
                            strx3 = strx3.replace("\'","")
                            #print(strx3)
                            tweet_ids_final.append(strx3)
                        database_entry.append(tweet_ids_final)

                    if(indx == 3): #mentions
                        mentions = i2.split(",")
                        final_mentions = []
                        for mention in mentions:
                            mention = mention.replace("[","")
                            mention = mention.replace("]","")
                            mention = mention.replace("'","")
                            mention = mention.replace(" ","")
                            final_mentions.append(mention)
                            #print(mention)
                        database_entry.append(final_mentions)
                
                    if(indx == 4): #locations
                        locations = i2.split(",")
                        final_locations = []
                        for location in locations:
                            if location in final_locations:
                                continue
                            location = location.replace("[","")
                            location = location.replace("]","")
                            location = location.replace("'","")
                            try:
                                location = location.lower()
                            except:
                                pass
                            d = enchant.Dict("en_US")
                            if (location):
                                if(location != ""):
                                    #print(location)
                                    loc_state_test = location.replace(" ", "")
                                    if loc_state_test in state_names: #State check
                                        final_locations.append(loc_state_test) #IS A STATE
                                    else:
                                        for suggestVar in d.suggest(location):
                                            suggestVar = suggestVar.replace(" ", "")
                                            if suggestVar.lower() in state_names:
                                                final_locations.append(loc_state_test) #IS A STATE
                                                break

                                    #ALSO DO LOCATION CHECK         
                                    location_resolved = locationResolver(location)
                                    if(location_resolved): #We have country and coordinates
                                        #print(location_resolved)
                                        final_locations.append(location_resolved)
                                        
                                    else: #If we can't easy resolve, hard resolve
                                        for suggestVar in d.suggest(location):
                                            location_resolved = locationResolver(suggestVar)
                                            if(location_resolved): #Hard resolve success
                                                final_locations.append(location_resolved)
                                                break
                                            
                                        #Hard resolve fail
                            #print(location)
                        database_entry.append(final_locations)

                    if(indx == 6): #emotions
                        emotions = i2.split(", ")
                        final_emotions = []
                        for emotion in emotions:
                            emotion = emotion.replace("[","")
                            emotion = emotion.replace("'","")
                            emotion = emotion.replace(" ","")
                            emotion = emotion.replace("]","")
                            final_emotions.append(round(float(emotion),4))
                            #print(emotion)
                        database_entry.append(final_emotions)

                    if(indx == 7): #word cloud
                        words = i2.split(", ")
                        final_words = []
                        for word in words:
                            word = word.replace("('","")
                            word = word.replace("'","")
                            word = word.replace(")","")
                            word = word.replace("[","")
                            word = word.replace("]","")
                            #print(word)
                            final_words.append(word)
                        database_entry.append(final_words)
                    if(indx == 8): #Tweet count
                        i2 = i2.replace("[","")
                        i2 = i2.replace("]","")
                        database_entry.append(i2)
                    
                    indx = indx + 1

                try:
                    #FILL EMOTIONS TABLE
                    #print("EMOTIONS")
                    sql = '''
                    INSERT INTO emotions (emotion_ID, emotion_VPOS, emotion_POS, emotion_NEU, emotion_NEG, emotion_VNEG)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    '''
                    val = (ID_definer, database_entry[5][0], database_entry[5][1], database_entry[5][2], database_entry[5][3], database_entry[5][4])
                    #print(database_entry[5][0])
                    exe = connection.cursor()
                    exe.execute(sql,val)
                    

                    #FILL HASHTAGS TABLE
                    #print("HASHTAGS")
                    hashtag_len = len(database_entry[1])
                    sql = '''
                    INSERT INTO hashtags (hashtag_ID, hashtag_1, hashtag_2, hashtag_3,
                    hashtag_4, hashtag_5, hashtag_6, hashtag_7, hashtag_8, hashtag_9, hashtag_10, hashtag_11, hashtag_12)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    val = [ID_definer]
                    loopamnt = 12
                    i = 0 
                    while (i < loopamnt):
                        #print("I:",i)
                        #print("LN:",hashtag_len)
                        if(i < hashtag_len):
                            tmp = database_entry[1][i]
                            tmp = tmp.replace("[","")
                            tmp = tmp.replace("]","")
                            try:
                                val.append(tmp)
                            except:
                                loopamnt += 1
                        else:
                            val.append("none")
                        i += 1
                    val = tuple(val)
                    exe = connection.cursor()
                    exe.execute(sql,val)

                    #FILL LOCATIONS INFORMATION ALL OF IT : LOCATIONS | STATES | COORDS
                    #print("LOCATIONS")
                    sql_location = '''
                    INSERT INTO locations (location_ID, location_1, location_2, location_3,
                    location_4, location_5, location_6, location_7, location_8, location_9, location_10, location_11, location_12,
                    location_13, location_14, location_15, location_16, location_17, location_18, location_19, location_20,
                    location_21, location_22, location_23, location_24, location_25, location_26, location_27, location_28,
                    location_29, location_30, location_31, location_32, location_33, location_34, location_35, location_36,
                    location_37, location_38, location_39, location_40)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''

                    sql_coord = '''
                    INSERT INTO coords (coords_ID, coord_1, coord_2, coord_3,
                    coord_4, coord_5, coord_6, coord_7, coord_8, coord_9, coord_10, coord_11, coord_12,
                    coord_13, coord_14, coord_15, coord_16, coord_17, coord_18, coord_19, coord_20,
                    coord_21, coord_22, coord_23, coord_24, coord_25, coord_26, coord_27, coord_28,
                    coord_29, coord_30, coord_31, coord_32, coord_33, coord_34, coord_35, coord_36,
                    coord_37, coord_38, coord_39, coord_40, coord_41, coord_42, coord_43, coord_44, coord_45,
                    coord_46, coord_47, coord_48, coord_49, coord_50, coord_51, coord_52, coord_53, coord_54,
                    coord_55, coord_56, coord_57, coord_58, coord_59, coord_60, coord_61, coord_62, coord_63,
                    coord_64, coord_65, coord_66, coord_67, coord_68, coord_69, coord_70, coord_71, coord_72,
                    coord_73, coord_74, coord_75, coord_76, coord_77, coord_78, coord_79, coord_80)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''

                    sql_states = '''
                    INSERT INTO states (states_ID, state_1, state_2, state_3,
                    state_4, state_5, state_6, state_7, state_8, state_9, state_10, state_11, state_12,
                    state_13, state_14, state_15, state_16, state_17, state_18, state_19, state_20,
                    state_21, state_22, state_23, state_24, state_25, state_26, state_27, state_28,
                    state_29, state_30, state_31, state_32, state_33, state_34, state_35, state_36,
                    state_37, state_38, state_39, state_40)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    
                    #create val dynamically, of length of array

                    ########
                    locations_extracted = []
                    coords_extracted = []
                    states_extracted = []

                            
                    for i in range(len(database_entry[4])):
                        entry = database_entry[4][i]
                        
                        if(len(entry) == 2): #Full entry
                            if(entry[0] not in locations_extracted):
                                locations_extracted.append(entry[0])
                            coords_extracted.append(entry[1])
                        else:
                            if( entry in state_names):
                                if(entry not in states_extracted):
                                    states_extracted.append(entry)
                    locations_len = len(locations_extracted)
                    coords_len = len(coords_extracted)
                    states_len = len(states_extracted)
                    ########
                    
                    #locations_extracted
                    val = [ID_definer]
                    loopamnt = 40
                    i = 0 
                    while (i < loopamnt):
                        if(i < locations_len):
                            try:
                                val.append(locations_extracted[i])
                            except:
                                loopamnt += 1
                        else:
                            val.append("none")
                        i += 1
                    #val = tuple(val)
                    exe = connection.cursor()
                    exe.execute(sql_location,val)
                    #coords
                    val = [ID_definer]
                    loopamnt = 80
                    i = 0
                    latlong_switch = True
                    while (i < loopamnt):
                        if(i < coords_len):
                            try:
                                if(latlong_switch):
                                    val.append(coords_extracted[i][0])
                                    latlong_switch = not latlong_switch
                                else:
                                    val.append(coords_extracted[i][1])
                                    latlong_switch = not latlong_switch
                            except:
                                loopamnt += 1
                        else:
                            val.append(0)
                        i += 1
                    #val = tuple(val)
                    exe = connection.cursor()
                    exe.execute(sql_coord,val)
                    #States
                    val = [ID_definer]
                    loopamnt = 40
                    i = 0 
                    while (i < loopamnt):
                        if(i < states_len):
                            try:
                                val.append(states_extracted[i])
                            except:
                                loopamnt += 1
                        else:
                            val.append("none")
                        i += 1
                    #val = tuple(val)
                    exe = connection.cursor()
                    exe.execute(sql_states,val)

                    #FILL MENTIONS TABLE
                    #print("MENTIONS")
                    mentions_len = len(database_entry[3])
                    sql = '''
                    INSERT INTO mentions (mention_ID, mention_1, mention_2, mention_3,
                    mention_4, mention_5, mention_6, mention_7, mention_8, mention_9, mention_10, mention_11, mention_12)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    val = [ID_definer]
                    loopamnt = 12
                    i = 0 
                    while (i < loopamnt):
                        if(i < mentions_len):
                            try:
                                val.append(database_entry[3][i])
                            except:
                                loopamnt += 1
                        else:
                            val.append("none")
                        i += 1
                    val = tuple(val)
                    exe = connection.cursor()
                    exe.execute(sql,val)

                    #FILL TWEET_IDS
                    #print("TWEETIDS")
                    tweetids_len = len(database_entry[2])
                    sql = '''
                    INSERT INTO tweetids (tweetid_ID, tweet_1, tweet_2, tweet_3,
                    tweet_4, tweet_5, tweet_6, tweet_7, tweet_8, tweet_9, tweet_10, tweet_11, tweet_12)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    #print(database_entry[2][0])
                    val = [ID_definer]
                    loopamnt = 12
                    i = 0 
                    while (i < loopamnt):
                        if(i < tweetids_len):
                            try:
                                val.append(database_entry[2][i])
                            except:
                                loopamnt += 1
                        else:
                            val.append("none")
                        i += 1
                    val = tuple(val)
                    exe = connection.cursor()
                    exe.execute(sql,val)

                    #FILL WORDCLOUD
                    #print("CLOUD")
                    wordlist_len = len(database_entry[6])
                    sql = '''
                    INSERT INTO wordcloud (wordcloud_ID, word_1, word_1_count, word_2, word_2_count, word_3,
                    word_3_count, word_4, word_4_count, word_5, word_5_count, word_6, word_6_count, word_7,
                    word_7_count, word_8, word_8_count, word_9, word_9_count, word_10, word_10_count, word_11, word_11_count, word_12,
                    word_12_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    val = [ID_definer]
                    loopamnt = 24
                    i = 0 
                    while (i < loopamnt):
                        if(i < wordlist_len):
                            try:
                                val.append(database_entry[6][i])
                            except:
                                loopamnt += 1
                        else:
                            val.append("none")
                        i += 1
                    val = tuple(val)
                    exe = connection.cursor()
                    exe.execute(sql,val)

                    sql = '''INSERT INTO trends (keyword, date, hashtags_ID, locations_ID, mentions_ID, emotions_ID, tweetids_ID, wordcloud_ID, occurrences, coords_id, states_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

                    f = f.replace(".csv","")
                    f = f.replace('C:\\Users\\helpu\\Desktop\\database loader\\',"")
                    f = f.replace('.json',"")
                    if (f in keyword_stop_words):
                        print("Keyword denied!")
                        continue
                    val = (f, database_entry[0], ID_definer, ID_definer, ID_definer, ID_definer, ID_definer, ID_definer, database_entry[7], ID_definer, ID_definer)

                    exe = connection.cursor()
                    exe.execute(sql,val)

                    #FINAL AND ONLY COMMIT
                    connection.commit()
                    print("Entry written successfully!")
                except Exception as e: 
                    print(e)
                    print("this entry had an error!")
                    #print(errorInfo)
                
            

            
        
        #sql = '''INSERT INTO trends (keyword, date, hashtags_ID, locations_ID, mentions_ID, emotions_ID, tweetids_ID, wordcloud_ID)
        #VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
                    
        #val = ('biden','01/01/2020', 1,1,1,1,1,1,)
        #mycursor = connection.cursor()
        #mycursor.execute(sql, val)
                    
        #connection.commit()


                
                
