procedure {:checksum "P0$proc#5"} P0();
requires F0();
ensures F0();
// Action: verify (procedure changed)
implementation {:checksum "P0$impl#0"} P0()
{
    call P0();
}


function {:checksum "F0#1"} F0() : bool
{
    false
}
