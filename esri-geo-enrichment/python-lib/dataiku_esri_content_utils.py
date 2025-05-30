# -*- coding: utf-8 -*-
from dataiku.customrecipe import *

################# TMP dict structure ##################
#### Based on: https://developers.arcgis.com/rest/geoenrichment/api-reference/data-collections.htm
#### and API : 
#### some basic facts changed in the API from by example : '["ATFacts","ATSpend"]' -to-> '["KeyFacts","Spending"]'  and some others did not changed
#### By consequence: some countries won't probably be enriched until all these values migrated into the API.
#######################################################


def get_esri_coverage(): 
   dict_esri_coverage = {}

   dict_esri_coverage['country']=['United States',
         'Albania',
         'Algeria',
         'Andorra',
         'Angola',
         'Argentina',
         'Armenia',
         'Aruba',
         'Australia',
         'Austria',
         'Azerbaijan',
         'Bahamas',
         'Bahrain',
         'Bangladesh',
         'Belarus',
         'Belgium',
         'Bermuda',
         'Bolivia',
         'Bosnia and Herzegovina',
         'Botswana',
         'Brazil',
         'Bulgaria',
         'Cameroon',
         'Canada',
         'Cayman Islands',
         'Chile',
         'China',
         'Colombia',
         'Costa Rica',
         "Cote d'Ivoire",
         'Croatia',
         'Cyprus',
         'Czech Republic',
         'Denmark',
         'Dominican Republic',
         'Ecuador',
         'Egypt',
         'El Salvador',
         'Estonia',
         'Faroe Islands',
         'Finland',
         'France',
         'French Polynesia',
         'Georgia',
         'Germany',
         'Ghana',
         'Greece',
         'Greenland',
         'Guadaloupe',
         'Guatemala',
         'Honduras',
         'Hong Kong',
         'Hungary',
         'Iceland',
         'India',
         'Indonesia',
         'Ireland',
         'Israel',
         'Italy',
         'Jamaica',
         'Japan',
         'Jordan',
         'Kazakhstan',
         'Kenya',
         'Kosovo',
         'Kuwait',
         'Kyrgyzstan',
         'Latvia',
         'Lebanon',
         'Lesotho',
         'Liechtenstein',
         'Lithuania',
         'Luxembourg',
         'Macao',
         'Malawi',
         'Malaysia',
         'Malta',
         'Martinique',
         'Mauritius',
         'Mexico',
         'Moldova',
         'Monaco',
         'Mongolia',
         'Montenegro',
         'Morocco',
         'Mozambique',
         'Namibia',
         'Netherlands',
         'New Caledonia',
         'New Zealand',
         'Nicaragua',
         'Nigeria',
         'Norway',
         'Oman',
         'Pakistan',
         'Panama',
         'Paraguay',
         'Peru',
         'Philippines',
         'Poland',
         'Portugal',
         'Puerto Rico',
         'Qatar',
         'Reunion',
         'Romania',
         'Russia',
         'Saudi Arabia',
         'Serbia',
         'Singapore',
         'Slovakia',
         'Slovenia',
         'South Africa',
         'South Korea',
         'Spain',
         'Sri Lanka',
         'Sudan',
         'Swaziland',
         'Sweden',
         'Switzerland',
         'Syria',
         'Taiwan',
         'Tajikistan',
         'Tanzania',
         'Thailand',
         'The Former Yugoslav Republic of Macedonia',
         'Trinidad and Tobago',
         'Tunisia',
         'Turkey',
         'Uganda',
         'Ukraine',
         'United Arab Emirates',
         'United Kingdom',
         'Uruguay',
         'Uzbekistan',
         'Venezuela',
         'Vietnam',
         'Zambia']

   dict_esri_coverage['isocode2']=['US',
   
         'AL',
         'DZ',
         'AD',
         'AO',
         'AR',
         'AM',
         'AW',
         'AU',
         'AT',
         'AZ',
         'BS',
         'BH',
         'BD',
         'BY',
         'BE',
         'BM',
         'BO',
         'BA',
         'BW',
         'BR',
         'BG',
         'CM',
         'CA',
         'KY',
         'CL',
         'CN',
         'CO',
         'CR',
         'CI',
         'HR',
         'CY',
         'CZ',
         'DK',
         'DO',
         'EC',
         'EG',
         'SV',
         'EE',
         'FO',
         'FI',
         'FR',
         'PF',
         'GE',
         'DE',
         'GH',
         'GR',
         'GL',
         'GP',
         'GT',
         'HN',
         'HK',
         'HU',
         'IS',
         'IN',
         'ID',
         'IE',
         'IL',
         'IT',
         'JM',
         'JP',
         'JO',
         'KZ',
         'KE',
         'XK',
         'KW',
         'KG',
         'LV',
         'LB',
         'LS',
         'LI',
         'LT',
         'LU',
         'MO',
         'MW',
         'MY',
         'MT',
         'MQ',
         'MU',
         'MX',
         'MD',
         'MC',
         'MN',
         'ME',
         'MA',
         'MZ',
         'NA',
         'NL',
         'NC',
         'NZ',
         'NI',
         'NG',
         'NO',
         'OM',
         'PK',
         'PA',
         'PY',
         'PE',
         'PH',
         'PL',
         'PT',
         'PR',
         'QA',
         'RE',
         'RO',
         'RU',
         'SA',
         'RS',
         'SG',
         'SK',
         'SI',
         'ZA',
         'KR',
         'ES',
         'LK',
         'SD',
         'SZ',
         'SE',
         'CH',
         'SY',
         'TW',
         'TJ',
         'TZ',
         'TH',
         'MK',
         'TT',
         'TN',
         'TR',
         'UG',
         'UA',
         'AE',
         'GB',
         'UY',
         'UZ',
         'VE',
         'VN',
         'ZM']

   dict_esri_coverage['isocode3']=['USA',
         'ALB',
         'DZA',
         'AND',
         'AGO',
         'ARG',
         'ARM',
         'ABW',
         'AUS',
         'AUT',
         'AZE',
         'BHS',
         'BHR',
         'BGD',
         'BLR',
         'BEL',
         'BMU',
         'BOL',
         'BIH',
         'BWA',
         'BRA',
         'BGR',
         'CMR',
         'CAN',
         'CYM',
         'CHL',
         'CHN',
         'COL',
         'CRI',
         'CIV',
         'HRV',
         'CYP',
         'CZE',
         'DNK',
         'DOM',
         'ECU',
         'EGY',
         'SLV',
         'EST',
         'FRO',
         'FIN',
         'FRA',
         'PYF',
         'GEO',
         'DEU',
         'GHA',
         'GRC',
         'GRL',
         'GLP',
         'GTM',
         'HND',
         'HKG',
         'HUN',
         'ISL',
         'IND',
         'IDN',
         'IRL',
         'ISR',
         'ITA',
         'JAM',
         'JPN',
         'JOR',
         'KAZ',
         'KEN',
         'XKS',
         'KWT',
         'KGZ',
         'LVA',
         'LBN',
         'LSO',
         'LIE',
         'LTU',
         'LUX',
         'MAC',
         'MWI',
         'MYS',
         'MLT',
         'MTQ',
         'MUS',
         'MEX',
         'MDA',
         'MCO',
         'MNG',
         'MNE',
         'MAR',
         'MOZ',
         'NAM',
         'NLD',
         'NCL',
         'NZL',
         'NIC',
         'NGA',
         'NOR',
         'OMN',
         'PAK',
         'PAN',
         'PRY',
         'PER',
         'PHL',
         'POL',
         'PRT',
         'PRI',
         'QAT',
         'REU',
         'ROU',
         'RUS',
         'SAU',
         'SRB',
         'SGP',
         'SVK',
         'SVN',
         'ZAF',
         'KOR',
         'ESP',
         'LKA',
         'SDN',
         'SWZ',
         'SWE',
         'CHE',
         'SYR',
         'TWN',
         'TJK',
         'TZA',
         'THA',
         'MKD',
         'TTO',
         'TUN',
         'TUR',
         'UGA',
         'UKR',
         'ARE',
         'GBR',
         'URY',
         'UZB',
         'VEN',
         'VNM',
         'ZMB']

      
      
   dict_esri_coverage['comment']=['nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'ESRI(r) Incomplete coverage due to trans-disputed areas, internal strife or regions that are sparsely populated.',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan',
         'nan']

   dict_esri_coverage['generic_datacollections']=['["KeyUSFacts"]'
      ,'["KeyFacts","Spending"]' #'["ALFacts","ALSpend"]'
      ,'["DZFacts"]'
      ,'["KeyFacts"]' #'["ADFacts"]'
      ,'["AOFacts"]'
      ,'["ARFacts"]'
      ,'["AMFacts"]'
      ,'["AWFacts"]'
      ,'["AUFacts","AUSpend"]'
      ,'["KeyFacts","Spending"]' #'["ATFacts","ATSpend"]'
      ,'["AZFacts"]'
      ,'["BSFacts"]'
      ,'["BHFacts","BUSpend"]'
      ,'["BDFacts"]'
      ,'["KeyFacts","Spending"]' #'["BYFacts","BYSpend"]'
      ,'["KeyFacts","Spending"]' #'["BEFacts","BESpend"]'
      ,'["BMFacts"]'
      ,'["BOFacts"]'
      ,'["KeyFacts","Spending"]' #'["BAFacts","BASpend"]'
      ,'["BWFacts"]'
      ,'["KeyFacts","Spending"]' #'["BRFacts","BRSpend"]'
      ,'["KeyFacts","Spending"]' #'["BGFacts","BGSpend"]'
      ,'["CMFacts"]'
      ,'["KeyCanFacts"]'
      ,'["KYFacts"]'
      ,'["CLFacts","CLSpend"]'
      ,'["KeyFacts","Spending"]' #'["CNFacts","CNSpend"]'
      ,'["COFacts","COSpend"]'
      ,'["CRFacts","CRSpend"]'
      ,'["CIFacts"]'
      ,'["KeyFacts","Spending"]' #'["HRFacts","HRSpend"]'
      ,'["KeyFacts","Spending"]' #'["CYFacts","CYSpend"]'
      ,'["KeyFacts","Spending"]' #'["CZFacts","CZSpend"]'
      ,'["KeyFacts","Spending"]' #'["DKFacts","DKSpend"]'
      ,'["DOFacts"]'
      ,'["ECFacts"]'
      ,'["EGFacts"]'
      ,'["KeyFacts"]' #'["SVFacts"]'
      ,'["KeyFacts","Spending"]' #'["EEFacts","EESpend"]'
      ,'["KeyFacts"]' #'["FOFacts"]'
      ,'["KeyFacts","Spending"]' #'["FIFacts","FISpend"]'
      ,'["KeyFacts","Spending"]' #'["FRFacts","FRSpend"]'
      ,'["PFFacts"]'
      ,'["GEFacts"]'
      ,'["KeyFacts","Spending"]' #["DEFacts","DESpend"]'
      ,'["GHFacts"]'
      ,'["KeyFacts","Spending"]' #'["GRFacts","GRSpend"]'
      ,'["GLFacts"]'
      ,'["GPFacts"]'
      ,'["GTFacts"]'
      ,'["HNFacts"]'
      ,'["HKFacts","HKSpend"]'
      ,'["KeyFacts","Spending"]' #'["HUFacts","HUSpend"]'
      ,'["KeyFacts","Spending"]' #'["ISFacts","ISSpend"]'
      ,'["INFacts","INSpend"]'
      ,'["IDFacts"]'
      ,'["KeyFacts","Spending"]' #'["IEFacts","IESpend"]'
      ,'["ILFacts","ILSpend"]'
      ,'["KeyFacts","Spending"]' #'["ITFacts","ITSpend"]'
      ,'["JMFacts"]'
      ,'["JPFacts","JPSpend"]'
      ,'["JOFacts"]'
      ,'["KZFacts"]'
      ,'["KEFacts"]'
      ,'["KeyFacts"]' #'["XKFacts"]'
      ,'["KWFacts"]'
      ,'["KGFacts"]'
      ,'["KeyFacts","Spending"]' #'["LVFacts","LVSpend"]'
      ,'["LBFacts"]'
      ,'["LSFacts"]'
      ,'["KeyFacts","Spending"]' #'["LIFacts","LISpend"]'
      ,'["KeyFacts","Spending"]' #'["LTFacts","LTSpend"]'
      ,'["KeyFacts","Spending"]' #'["LUFacts","LUSpend"]'
      ,''
      ,'["MWFacts"]'
      ,'["MYFacts","MYSpend"]'
      ,'["KeyFacts","Spending"]' #'["MTFacts","MTSpend"]'
      ,'["MQFacts"]'
      ,'["MUFacts","MUSpend"]'
      ,'["MXFacts","MXSpend"]'
      ,'["KeyFacts","Spending"]' #'["MDFacts","MDSpend"]'
      ,'["KeyFacts"]' #'["MCFacts"]'
      ,'["MNFacts"]'
      ,'["KeyFacts","Spending"]' #'["MEFacts","MESpend"]'
      ,'["MAFacts"]'
      ,'["MZFacts"]'
      ,'["NAFacts"]'
      ,'["KeyFacts","Spending"]' #'["NLFacts","NLSpend"]'
      ,'["NCFacts"]'
      ,'["NZFacts","NZSpend"]'
      ,'["KeyFacts"]' #'["NIFacts"]'
      ,'["NGFacts"]'
      ,'["KeyFacts","Spending"]' #'["NOFacts","NOSpend"]'
      ,'["OMFacts"]'
      ,'["PKFacts"]'
      ,'["PAFacts"]'
      ,'["PYFacts"]'
      ,'["PEFacts"]'
      ,'["PHFacts","PHSpend"]'
      ,'["KeyFacts","Spending"]' #'["PLFacts","PLSpend"]'
      ,'["KeyFacts","Spending"]' #'["PTFacts","PTSpend"]'
      ,'["PRFacts"]'
      ,'["QAFacts"]'
      ,''
      ,'["KeyFacts","Spending"]' #'["ROFacts","ROSpend"]'
      ,'["KeyFacts","Spending"]' #'["RUFacts","RUSpend"]'
      ,'["SAFacts","SASpend"]'
      ,'["KeyFacts","Spending"]' #'["RSFacts","RSSpend"]'
      ,'["SGFacts","SGSpend"]'
      ,'["KeyFacts","Spending"]' #'["SKFacts","SKSpend"]'
      ,'["KeyFacts","Spending"]' #'["SIFacts","SISpend"]'
      ,'["ZAFacts"]'
      ,'["KRFacts","KRSpend"]'
      ,'["KeyFacts","Spending"]' #'["ESFacts","ESSpend"]'
      ,'["LKFacts"]'
      ,'["SDFacts"]'
      ,'["SZFacts"]'
      ,'["KeyFacts","Spending"]' #'["SEFacts","SESpend"]'
      ,'["KeyFacts","Spending"]' #'["CHFacts","CHSpend"]'
      ,'["SYFacts"]'
      ,'["TWFacts","TWSpend"]'
      ,'["TJFacts"]'
      ,'["TZFacts"]'
      ,'["THFacts","THSpend"]'
      ,'["KeyFacts","Spending"]' #''
      ,'["TTFacts"]'
      ,'["TNFacts"]'
      ,'["KeyFacts","Spending"]' #'["TRFacts","TRSpend"]'
      ,'["UGFacts"]'
      ,'["KeyFacts","Spending"]' #'["UAFacts","UASpend"]'
      ,'["AEFacts"]'
      ,'["KeyFacts","Spending"]' #'["GBFacts","GBSpend"]',
      ,'["UYFacts"]'
      ,''
      ,'["VEFacts"]'
      ,'["VNFacts"]'
      ,'["ZMFacts"]']
      
      
   dict_esri_coverage['esri_content_as_of']=['2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17',
         '2016-02-17']


   dict_esri_coverage['local_collections']=['["lifemodegroupsNEW","TravelCEX"]',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      ''
   ]
      
      
   dict_esri_coverage['landscape_collections']=['["LandscapeFacts"]',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      ''
   ]
      

   return dict_esri_coverage