#
# Delta process, cycle tests
#

umask 00
tstdir=/home/user0/DataProcess/cycle-tests
tmpdir=/tmp/rundeck/DataProcess/cycle-tests
mkdir -p $tmpdir
chmod -R 777 $tmpdir


# warning level scripts
echo -:- warning tests...
for tscript in `ls $tstdir/warnings` ; do
  echo -: $tscript
  psql -d demo -f "$tstdir/warnings/$tscript" | head -5 > $tmpdir/$tscript.out
  lcount=`wc -l $tmpdir/$tscript.out | cut -f1 -d' '`
  # 5 is a critical number: there is at least 1 line in SQL output
  if [ $lcount -ge 5 ] ; then
    echo Warning! record sample:
    cat $tmpdir/$tscript.out
  fi
done


# error level scripts
rc=0
echo -:- error tests...
for tscript in `ls $tstdir/errors` ; do
  echo -: $tscript
  psql -d demo -f "$tstdir/errors/$tscript" | head -5 > $tmpdir/$tscript.out
  lcount=`wc -l $tmpdir/$tscript.out | cut -f1 -d' '`
  # 5 is a critical number: there is at least 1 line in SQL output
  if [ $lcount -ge 5 ] ; then
    echo Error! record sample:
    cat $tmpdir/$tscript.out
    rc=1
  fi
done

# return code depends on errors only
exit $rc
