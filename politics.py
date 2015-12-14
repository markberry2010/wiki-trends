# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 15:41:57 2015

@author:    Mark Berry
            Global Monetary and Sovereign Markets
            mark.berry@frb.gov
"""

from datetime import *

import pandas as pd
from ggplot import *

from wiki_download import WikiDownloader

debates = [datetime(2015,8,6,21),
           datetime(2015,9,16, 21),
           datetime(2015,10,28, 21),
           datetime(2015,11,10, 21)]

candidates = ['Jeb_Bush',
              'Ben_Carson',
              'Chris_Christie',
              'Ted_Cruz',
              'Carly_Fiorina',
              'Jim_Gilmore',
              'Lindsey_Graham',
              'Mike_Huckabee',
              'John_Kasich',
              'George_Pataki',
              'Rand_Paul',
              'Rick_Santorum',
              'Donald_Trump',
              'Hillary_Clinton',
              'Bernie_Sanders',
              'Martin_O\'Malley']
           
#dl = WikiDownloader(debates[0], debates[0] + timedelta(12), names = candidates)
#dl.download_data('U:/GMSM/Berry/wiki/politics.h5')

data = pd.read_hdf('U:/GMSM/Berry/wiki/politics.h5',
                  'df')
data.name = data.name.str.replace('_',' ') #Remove underscores from names
en=data.ix[data.project=='en']

y = (ggplot(en.reset_index(),aes('index','hits','name'))
        +geom_line(size=2)
        +labs(x='GMT',y='Pageviews',
              title='Wikipeida Pageviews During First Republican Debate'))
print (y)
