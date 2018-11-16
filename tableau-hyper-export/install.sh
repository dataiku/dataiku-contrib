#! /bin/sh

echo "Starting Tableau plugin installation procedure"

if [ -z "$DIP_HOME" ]; then
	echo "Missing DIP_HOME"
	exit 1
fi

detectedDistrib=$($DKUINSTALLDIR/scripts/_find-distrib.sh)
distrib=`echo "$detectedDistrib"|cut -d ' ' -f 1`

USEDPYTHONBIN=$DIP_HOME/code-envs/python/plugin_tableau-hyper-export_managed/bin

echo "Used python bin is $USEDPYTHONBIN"

if [ ! -d "$USEDPYTHONBIN" ]; then
	echo "Plugin codenv plugin_tableau-hyper-export_managed does not exist"
    echo "You may want to create it or make sure the name is correct"
	exit 1
fi

isHyper=$(
$USEDPYTHONBIN/python - <<EOF
try:
    import tableausdk.HyperExtract
    print 1
except Exception:
    print 0
EOF
)
echo "is hyper is $isHyper"

if [ "$isHyper" == 1 ]; then
    echo "Tableau SDK HyperExtract package already installed, skipping installation"
    exit 0
else
    echo "Installing Tableau extract api"
fi

TMPDIR=`mktemp -d $DIP_HOME/tmp/tableau-install-XXXXXX`
cd $TMPDIR
echo $TMPDIR

echo "Downloading"
case "$distrib" in
	osx)
		curl "https://downloads.tableau.com/tssoftware/extractapi-py-osx64-2018-2-0.tar.gz" |tar --strip-components 1 -x -z -f -
		;;
	*)
		curl "https://downloads.tableau.com/tssoftware/extractapi-py-linux-x86_64-2018-2-0.tar.gz" |tar --strip-components 1 -x -z -f -
		;;
esac

echo "Download done, building in $PWD"
$USEDPYTHONBIN/python setup.py build
echo "Built done, installing"
$USEDPYTHONBIN/python setup.py install

exit 0


