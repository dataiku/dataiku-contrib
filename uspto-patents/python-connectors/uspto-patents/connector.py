from dataiku.connector import Connector
import json, os

ipgfiles = """http://storage.googleapis.com/patents/grant_full_text/2015/ipg150106.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150113.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150120.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150127.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150203.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150210.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150217.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150224.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150303.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150310.zip
http://storage.googleapis.com/patents/grant_full_text/2015/ipg150317.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140107.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140114.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140121.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140128.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140204.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140211.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140218.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140225.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140304.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140311.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140318.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140325.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140401.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140408.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140415.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140422.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140429.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140506.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140513.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140520.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140527.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140603.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140610.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140617.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140624.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140701.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140708.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140715.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140722.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140729.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140805.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140812.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140819.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140826.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140902.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140909.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140916.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140923.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg140930.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141007.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141014.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141021.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141028.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141104.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141111.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141118.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141125.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141202.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141209.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141216.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141223.zip
http://storage.googleapis.com/patents/grant_full_text/2014/ipg141230.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130101.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130108.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130115.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130122.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130129.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130205.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130212.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130219.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130226.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130305.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130312.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130319.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130326.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130402.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130409.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130416.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130423.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130430.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130507.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130514.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130521.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130528.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130604.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130611.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130618.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130625.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130702.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130709.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130716.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130723.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130730.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130806.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130813.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130820.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130827.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130903.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130910.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130917.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg130924.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131001.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131008.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131015.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131022.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131029.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131105.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131112.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131119.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131126.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131203.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131210.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131217.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131224.zip
http://storage.googleapis.com/patents/grant_full_text/2013/ipg131231.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120103.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120110.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120117.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120124.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120131.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120207.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120214.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120221.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120228.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120306.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120313.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120320.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120327.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120403.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120410.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120417.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120424.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120501.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120508.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120515.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120522.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120529.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120605.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120612.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120619.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120626.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120703.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120710.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120717.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120724.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120731.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120807.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120814.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120821.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120828.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120904.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120911.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120918.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg120925.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121002.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121009.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121016.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121023.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121030.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121106.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121113.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121120.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121127.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121204.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121211.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121218.zip
http://storage.googleapis.com/patents/grant_full_text/2012/ipg121225.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110104.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110111.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110118.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110125.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110201.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110208.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110215.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110222.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110301.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110308.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110315.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110322.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110329.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110405.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110412.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110419.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110426.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110503.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110510.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110517.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110524.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110531.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110607.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110614.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110621.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110628.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110705.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110712.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110719.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110726.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110802.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110809.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110816.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110823.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110830.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110906.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110913.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110920.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg110927.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111004.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111011.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111018.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111025.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111101.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111108.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111115.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111122.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111129.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111206.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111213.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111220.zip
http://storage.googleapis.com/patents/grant_full_text/2011/ipg111227.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100105.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100112.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100119.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100126.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100202.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100209.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100216.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100223.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100302.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100309.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100316.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100323.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100330.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100406.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100413.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100420.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100427.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100504.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100511.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100518.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100525.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100601.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100608.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100615.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100622.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100629.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100706.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100713.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100720.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100727.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100803.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100810.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100817.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100824.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100831.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100907.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100914.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100921.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg100928.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101005.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101012.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101019.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101026.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101102.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101109.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101116.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101123.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101130.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101207.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101214.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101221.zip
http://storage.googleapis.com/patents/grant_full_text/2010/ipg101228.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090106.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090113.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090120.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090127.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090203.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090210.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090217.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090224.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090303.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090310.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090317.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090324.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090331.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090407.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090414.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090421.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090428.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090505.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090512.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090519.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090526.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090602.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090609.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090616.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090623.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090630.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090707.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090714.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090721.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090728.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090804.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090811.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090818.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090825.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090901.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090908.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090915.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090922.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg090929.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091006.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091013.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091020.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091027.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091103.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091110.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091117.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091124.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091201.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091208.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091215.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091222.zip
http://storage.googleapis.com/patents/grant_full_text/2009/ipg091229.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080101.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080108.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080115.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080122.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080129.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080205.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080212.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080219.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080226.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080304.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080311.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080318.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080325.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080401.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080408.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080415.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080422.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080429.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080506.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080513.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080520.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080527.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080603.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080610.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080617.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080624.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080701.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080708.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080715.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080722.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080729.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080805.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080812.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080819.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080826.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080902.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080909.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080916.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080923.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg080930.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081007.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081014.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081021.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081028.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081104.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081111.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081118.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081125.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081202.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081209.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081216.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081223.zip
http://storage.googleapis.com/patents/grant_full_text/2008/ipg081230.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070102.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070109.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070116.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070123.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070130.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070206.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070213.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070220.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070227.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070306.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070313.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070320.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070327.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070403.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070410.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070417.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070424.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070501.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070508.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070515.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070522.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070529.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070605.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070612.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070619.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070626.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070703.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070710.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070717.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070724.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070731.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070807.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070814.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070821.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070828.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070904.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070911.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070918.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg070925.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071002.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071009.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071016.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071023.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071030.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071106.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071113.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071120.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071127.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071204.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071211.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071218.zip
http://storage.googleapis.com/patents/grant_full_text/2007/ipg071225.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060103.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060110.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060117.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060124.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060131.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060207.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060214.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060221.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060228.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060307.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060314.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060321.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060328.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060404.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060411.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060418.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060425.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060502.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060509.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060516.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060523.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060530.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060606.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060613.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060620.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060627.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060704.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060711.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060718.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060725.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060801.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060808.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060815.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060822.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060829.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060905.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060912.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060919.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg060926.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061003.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061010.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061017.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061024.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061031.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061107.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061114.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061121.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061128.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061205.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061212.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061219.zip
http://storage.googleapis.com/patents/grant_full_text/2006/ipg061226.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050104.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050111.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050118.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050125.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050201.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050208.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050215.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050222.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050301.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050308.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050315.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050322.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050329.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050405.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050412.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050419.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050426.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050503.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050510.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050517.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050524.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050531.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050607.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050614.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050621.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050628.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050705.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050712.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050719.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050726.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050802.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050809.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050816.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050823.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050830.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050906.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050913.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050920.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg050927.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051004.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051011.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051018.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051025.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051101.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051108.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051115.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051122.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051129.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051206.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051213.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051220.zip
http://storage.googleapis.com/patents/grant_full_text/2005/ipg051227.zip
"""


import re
def extract_xml_strings(filename):
    """
    Given a string [filename], opens the file and returns a generator
    that yields tuples. A tuple is of format (year, xmldoc string). A tuple
    is returned for every valid XML doc in [filename]
    """
    # search for terminating XML tag
    endtag_regex = re.compile('^<!DOCTYPE (.*) SYSTEM')
    endtag = ''
    import zipfile
    import os
    try: 
        z = zipfile.ZipFile(filename, 'r')
    except zipfile.BadZipfile as e:
        print e
        return
    xmlfilename = os.path.basename(filename)[:-4] + ".xml"
    with z.open(xmlfilename, 'r') as f:
        doc = ''  # (re)initialize current XML doc to empty string
        for line in f:
            doc += line
            endtag = endtag_regex.findall(line) if not endtag else endtag
            if not endtag:
                continue
            terminate = re.compile('^</{0}>'.format(endtag[0]))
            if terminate.findall(line):
                yield doc
                endtag = ''
                doc = ''
    z.close()

# This follows google's rules for conversion of XML to JSON

def iterNodes(node, parentDict):
    nodeDict = {}
    try:
        nodeDict.update(node.attrib)
    except AttributeError:
        pass
    if node.text != None:
        nodeDict['text'] = node.text

    for i in node.iterchildren():
        childDict = {}
        newDict = {}
        newDict = iterNodes(i, childDict)
        newList = []
        if i.tag in nodeDict:
            try:
                nodeDict[i.tag].append(newDict[i.tag])
            except:
                newList.append(nodeDict[i.tag])
                nodeDict[i.tag] = newList
                nodeDict[i.tag].append(newDict[i.tag])
        else:
            nodeDict.update(newDict)
    tagList = node.tag.split(':')
    namespace = '$'.join(tagList)
    parentDict[namespace] = nodeDict
    if len(nodeDict) == 1 and 'text' in nodeDict: 
        parentDict[namespace] = nodeDict['text']
    return parentDict


"""
Ths custom connector itself
"""
class USPTOConnector(Connector):

    def __init__(self, config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor
        """
        Connector.__init__(self, config)  # pass the parameters to the base class

        self.cache_folder = self.config.get("cache_folder")
        self.test_mode = self.config["test_mode"]
        self.all_years = self.config["all_years"]

        print 'Running Patent Connector cache=%s test=%s all=%s' % (self.cache_folder, self.test_mode, self.all_years)

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        Whether additional columns returned by the generate_rows are kept is configured
        in the connector.json with the "strictSchema" field
        """
        return {"columns":[{"name":"patent", "type": "string"}]}


    def files(self, partition_id):
        import re
        import os.path
        for f in ipgfiles.split('\n'):
            p = re.findall('/(\d\d\d\d)/', f)
            if len(p) == 0:
                continue
            filename = os.path.basename(f)
            if not filename:
                continue

            # Zap unselected years if needed
            if not self.all_years and partition_id != p[0]:
                print "Skipping", f, p
                continue

            yield (f, filename, p)

    def download(self, url, filename):
        import urllib
        try:
            urllib.urlretrieve(url, filename)
            return filename
        except IOError as e:
            print e
            return None


    def get_filename(self, url, filename):
        import os.path

        if not os.path.isdir(self.cache_folder):
            os.makedirs(self.cache_folder)

        p = os.path.join(self.cache_folder, filename)
        if os.path.exists(p) and os.path.isfile(p) and os.path.getsize(p) > 0:
            print "  Cache hit"
            return p
        print "  Downloading"
        k  = self.download(url, p)
        print "  Downloaded"
        if k:
            return p
        else:
            return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        limit_mode = False
        if records_limit != -1 or self.test_mode:
            limit_mode = True
        ### We hard force limit to 100 because of the time required for parsing ...

        if not self.all_years and partition_id not in self.list_partitions(None):
            raise ValueError("Unexpected partition id: '%s' - expected one of %s" % (partition_id, ",".join(self.list_partitions(None))))

        count = 0
        for (url, filename, year) in self.files(partition_id):
            print filename
            fullname = self.get_filename(url, filename)
            if not fullname:
                continue
            if limit_mode and count > 100:
                break 
            for doc in extract_xml_strings(fullname):
                count = count + 1
                if count % 1000 == 0:
                    print "Patents : parsed", count, " lines"
                if limit_mode and count > 100:
                    break 
                emptyDict = {}
                from lxml import objectify
                from StringIO import StringIO 
                o = objectify.parse(StringIO(doc))
                iterNodes(o.getroot(), emptyDict)
                s = json.dumps(emptyDict)
                yield { "patent" : s, "year" : year[0], "filename" : filename}


    def get_partitioning(self):
        if self.all_years:
            return None
        else:
            return {
                "dimensions": [
                    {
                        "name" : "year",
                        "type" : "time",
                        "params" : {
                            "period" : "YEAR"
                        }

                    }
                ]
            }

    def list_partitions(self, dataset_partitioning):
        if self.all_years:
            return []
        else:
            return [str(x) for x in xrange(2005, 2016)]