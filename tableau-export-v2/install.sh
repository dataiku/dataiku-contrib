#! /bin/sh

echo "Starting Tableau plugin installation procedure"

if [ -z "$DIP_HOME" ]
then
	echo "Missing DIP_HOME"
	exit 1
fi

detectedDistrib=$($DKUINSTALLDIR/scripts/_find-distrib.sh)
distrib=`echo "$detectedDistrib"|cut -d ' ' -f 1`

TMPDIR=`mktemp -d $DIP_HOME/tmp/tableau-install-XXXXXX`
cd $TMPDIR
echo $TMPDIR

echo "Downloading"
case "$distrib" in
	osx)
		curl "http://downloads.tableau.com/tssoftware/Tableau-SDK-Python-OSX-64Bit-9-2-4.tar.gz" |tar --strip-components 1 -x -z -f -
		;;
	*)
		curl "http://downloads.tableau.com/tssoftware/Tableau-SDK-Python-Linux-64Bit-9-2-4.tar.gz" |tar --strip-components 1 -x -z -f -
		;;
esac

echo "Download done, doing install"
$DIP_HOME/bin/python setup.py install
