#!/bin/bash

# Tests to ensure that the Java and Haskell versions of SSync produce
# byte-for-byte equivalent results for the same input.

set -e

cabal build
sbt clean package

function hsexe {
    dist/build/datasync-worker/datasync-worker "$@"
}
function jvexe {
    java -jar target/ssync-*.jar "$@"
}

function qdd {
    dd "$@" >/dev/null 2>/dev/null
}

function check {
    if ! cmp "$1" "$2"; then
        echo "Press enter..."
        read
        exit 1
    fi
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

cat "$tmpDir/bits1" "$tmpDir/bits2" "$tmpDir/bits3" "$tmpDir/bits4" > "$tmpDir/bits"

BS=$((RANDOM + 700))
FRAGSZ=$((RANDOM / 100 + 1))

qdd if=/dev/urandom of="$tmpDir/frag" bs=$FRAGSZ count=1

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

function scmp {
    hsexe "$1" "$2" "$3-hs" "$4-hs"
    jvexe "$1" "$2" "$3-jv" "$4-jv"
    check "$4-hs" "$4-jv"
}

function diffpatch {
    local label="$1"
    local target="$2"
    echo "...when diffing"
    scmp diff "$tmpDir/$target" "$tmpDir/$label/bits.ssig" "$tmpDir/$label/$target.sdiff"
    echo "...when patching"
    scmp patch "$tmpDir/bits" "$tmpDir/$label/$target.sdiff" "$tmpDir/$label/$target.out"
    echo "...final result"
    check "$tmpDir/$label/$target.out-hs" "$tmpDir/$target"
}

function run {
    local label="test-$1"
    local chk="$2"
    local strong="$3"

    mkdir "$tmpDir/$label"

    echo "Signatures"
    hsexe sig --chk MD5 --strong MD5 --bs $BS "$tmpDir/bits" "$tmpDir/$label/bits.ssig-hs"
    jvexe sig --chk MD5 --strong MD5 --bs $BS "$tmpDir/bits" "$tmpDir/$label/bits.ssig-jv"
    check "$tmpDir/$label/bits.ssig-hs" "$tmpDir/$label/bits.ssig-jv"

    echo "No change"
    diffpatch "$label" bits

    echo "Start insert"
    diffpatch "$label" bits-start-insert
    echo "Middle insert"
    diffpatch "$label" bits-mid-insert
    echo "End insert"
    diffpatch "$label" bits-end-insert

    echo "Start delete"
    diffpatch "$label" bits-start-delete
    echo "Middle delete"
    diffpatch "$label" bits-mid-delete
    echo "End delete"
    diffpatch "$label" bits-end-delete

    echo "Start replace"
    diffpatch "$label" bits-start-replace
    echo "Middle replace"
    diffpatch "$label" bits-mid-replace
    echo "End replace"
    diffpatch "$label" bits-end-replace

    echo "Reverse"
    diffpatch "$label" bits-reverse
}

run samehash MD5 MD5
run dhash1 MD5 SHA1
run dhash2 SHA1 MD5
