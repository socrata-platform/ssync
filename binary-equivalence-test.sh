#!/bin/bash

# Tests to ensure that the Java and Haskell versions of SSync produce
# byte-for-byte equivalent results for the same input.

tmp=`env GETOPT_COMPATIBLE=1 getopt io:u: "$@"`
if [ $? != 0 ]; then
    echo "Usage: $0 [-u PROBLEMFILE] [-o PROBLEMFILE] -i"
    echo
    echo "If a test fails and -o is specified, the PROBLEMFILE will be created"
    echo "containing the test data which caused the failure."
    echo
    echo "If -u is specified, the PROBLEMFILE will be used to reproduce a previously"
    echo "failed test run."
    echo
    echo "When -i is specified, a failing test will cause the script to"
    echo "pause before exiting."
    exit 1
fi

eval set -- "$tmp"

unset problemfile
interactive=false
unset usedata
while true; do
      case "$1" in
          "-i")
              interactive=true
              shift;;
          "-o")
              problemfile="$2"
              shift 2;;
          "-u")
              usedata="$2"
              shift 2;;
          "--")
              shift
              break;;
      esac
done

set -e

stack build
sbt clean package

function hsexe {
    stack exec -- ssync "$@"
}
function jvexe {
    java -jar target/ssync-*.jar "$@"
}

function qdd {
    dd "$@" >/dev/null 2>/dev/null
}

if ! which mktemp >/dev/null; then
    echo "Script requires mktemp"
    exit 1
fi

tmpDir=$(mktemp -d tmp.XXXXXXXX)
function cleanup {
  rm -r "$tmpDir"
}
trap cleanup EXIT

if [ "x$usedata" = "x" ]; then
    BITSSZ1=$((RANDOM+10000))
    BITCNT1=$((RANDOM / 100 + 1))
    BITSSZ2=$((RANDOM+10000))
    BITCNT2=$((RANDOM / 100 + 1))
    BITSSZ3=$((RANDOM+10000))
    BITCNT3=$((RANDOM / 100 + 1))
    BITSSZ4=$((RANDOM+10000))
    BITCNT4=$((RANDOM / 100 + 1))

    qdd if=/dev/urandom of="$tmpDir/bits1" bs=$BITSSZ1 count=$BITCNT1
    qdd if=/dev/urandom of="$tmpDir/bits2" bs=$BITSSZ2 count=$BITCNT2
    qdd if=/dev/urandom of="$tmpDir/bits3" bs=$BITSSZ3 count=$BITCNT3
    qdd if=/dev/urandom of="$tmpDir/bits4" bs=$BITSSZ4 count=$BITCNT4

    BS=$((RANDOM + 700))
    FRAGSZ=$((RANDOM / 100 + 1))

    qdd if=/dev/urandom of="$tmpDir/frag" bs=$FRAGSZ count=1
else
    tar -xf "$usedata" -C "$tmpDir"

    # hmph, this use of "stat" is a GNUism; it won't work on OSX with
    # its standard 'stat'.
    if [ -e "$tmpDir/BS" ] && [ $(stat -c '%s' "$tmpDir/BS") -lt 7 ]; then
        BS="$(tr -C -d 0123456789 < "$tmpDir/BS")"
    else
        echo "Corrupt test file $usedata" >&2
    fi
fi

cat "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/bits3" "$tmpDir/bits4" > "$tmpDir/bits"

cat "$tmpDir/frag" "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/bits3" "$tmpDir/bits4" > "$tmpDir/bits-start-insert"
cat "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/frag" "$tmpDir/bits3" "$tmpDir/bits4" > "$tmpDir/bits-mid-insert"
cat "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/bits3" "$tmpDir/bits4" "$tmpDir/frag" > "$tmpDir/bits-end-insert"

cat "$tmpDir/bits2" "$tmpDir/bits3" "$tmpDir/bits4" > "$tmpDir/bits-start-delete"
cat "$tmpDir/bits1" "$tmpDir/bits4" > "$tmpDir/bits-mid-delete"
cat "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/bits3" > "$tmpDir/bits-end-delete"

cat "$tmpDir/frag" "$tmpDir/bits2" "$tmpDir/bits3" "$tmpDir/bits4" > "$tmpDir/bits-start-replace"
cat "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/frag" "$tmpDir/bits4" > "$tmpDir/bits-mid-replace"
cat "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/bits3" "$tmpDir/frag" > "$tmpDir/bits-end-replace"

cat "$tmpDir/bits4" "$tmpDir/bits3" "$tmpDir/bits2" "$tmpDir/bits1" > "$tmpDir/bits-reverse"

function check {
    local label="$1"
    local sublabel="$2"
    local subsublabel="$3"
    shift 3

    if ! cmp "$1" "$2"; then
        echo "$label - $sublabel - $subsublabel" > "$tmpDir/FAILURE"
        echo "$BS" > "$tmpDir/BS"
        if [ "x$problemfile" != "x" ]; then
            # No point compressing, it's almost all random data
            tar -cf "./$problemfile" -C "$tmpDir" {FAILURE,BS,bits1,bits2,bits3,bits4,frag}
        fi
        if $interactive; then
            echo "Press enter..."
            read
        fi
        exit 1
    fi
}

function scmp {
    local label="$1"
    local sublabel="$2"
    shift 2
    hsexe "$1" "$2" "$3-hs" "$4-hs"
    jvexe "$1" "$2" "$3-jv" "$4-jv"
    check "$label" "$sublabel" "$1" "$4-hs" "$4-jv"
}

function diffpatch {
    local label="$1"
    local sublabel="$2"
    local target="$3"

    echo "$label - $sublabel"
    echo "...when diffing"
    scmp "$label" "$sublabel" diff "$tmpDir/$target" "$tmpDir/$label/bits.ssig" "$tmpDir/$label/$target.sdiff"
    echo "...when patching"
    scmp "$label" "$sublabel" patch "$tmpDir/bits" "$tmpDir/$label/$target.sdiff" "$tmpDir/$label/$target.out"
    echo "...final result"
    check "$label" "$sublabel" "final result" "$tmpDir/$label/$target.out-hs" "$tmpDir/$target"
}

function run {
    local chk="$1"
    local strong="$2"
    local label="$chk + $strong"

    mkdir "$tmpDir/$label"

    echo "Signatures"
    hsexe sig --chk "$chk" --strong "$strong" --bs "$BS" "$tmpDir/bits" "$tmpDir/$label/bits.ssig-hs"
    jvexe sig --chk "$chk" --strong "$strong" --bs "$BS" "$tmpDir/bits" "$tmpDir/$label/bits.ssig-jv"
    check "$label" "Signatures" "" "$tmpDir/$label/bits.ssig-hs" "$tmpDir/$label/bits.ssig-jv"

    diffpatch "$label" "No change" bits

    diffpatch "$label" "Start insert" bits-start-insert
    diffpatch "$label" "Middle insert" bits-mid-insert
    diffpatch "$label" "End insert" bits-end-insert

    diffpatch "$label" "Start delete" bits-start-delete
    diffpatch "$label" "Middle delete" bits-mid-delete
    diffpatch "$label" "End delete" bits-end-delete

    diffpatch "$label" "Start replace" bits-start-replace
    diffpatch "$label" "Middle replace" bits-mid-replace
    diffpatch "$label" "End replace" bits-end-replace

    diffpatch "$label" "Reverse" bits-reverse
}

hashes=(MD5 SHA1 SHA-256 SHA-512)

for hash_a in "${hashes[@]}"; do
    for hash_b in "${hashes[@]}"; do
        run "$hash_a" "$hash_b"
    done
done
