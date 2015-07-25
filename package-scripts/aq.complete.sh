#!/bin/bash

_aq_complete() {
    local cur xspec

    COMPREPLY=()
    _get_comp_words_by_ref cur prev
    
    case "$cur" in
    -*)
	    COMPREPLY=( $( compgen -W '-f -l -j -n -h --condition --limit \
				    --input-file --fields --print-file --help --version \
				    --jobs --count-only --record-separator --field-separator \
				    --disable-parse-loop' -- $cur ) )
	    return 0
	    ;;
    esac


   #if [ "g$prev" = "g-f" ]; then
        
    #fi
    xspec="*.avro"
    xspec="!"$xspec

    _expand || return 0
    _compopt_o_filenames
    
    COMPREPLY=( $( compgen -f -X "$xspec" -- "$cur" ) $( compgen -d -- "$cur" ) )
    
    return 0
}

complete -F _aq_complete -o filenames aq
