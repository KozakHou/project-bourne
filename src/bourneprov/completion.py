"""Dependency-free shell completion generation and candidate lookup."""

from __future__ import annotations

from .storage import ExperimentStore

_RELATIVE_CANDIDATE_LIMIT = 20
_ID_CANDIDATE_LIMIT = 100


def experiment_candidates(store: ExperimentStore, prefix: str = "") -> list[str]:
    """Return completion candidates for the configured experiment database."""

    candidates: list[str] = []
    folded = prefix.casefold()
    total = store.count()
    if total and "latest".startswith(folded):
        candidates.append("latest")

    available = min(total, _RELATIVE_CANDIDATE_LIMIT)
    candidates.extend(
        reference
        for reference in (f"@{position}" for position in range(1, available + 1))
        if reference.startswith(prefix)
    )

    if not prefix.startswith("@") and not "latest".startswith(folded):
        candidates.extend(
            store.find_ids_by_prefix(prefix.upper(), limit=_ID_CANDIDATE_LIMIT)
        )
    elif not prefix:
        candidates.extend(store.find_ids_by_prefix("", limit=_ID_CANDIDATE_LIMIT))
    return candidates


def completion_script(shell: str) -> str:
    """Generate a completion definition for Bash, Zsh, or Fish."""

    try:
        return _SCRIPTS[shell]
    except KeyError:
        raise ValueError(f"unsupported shell: {shell}") from None


_SCRIPTS = {
    "bash": r'''_bourne_completion() {
    local current command
    COMPREPLY=()
    current="${COMP_WORDS[COMP_CWORD]}"
    command="${COMP_WORDS[1]}"

    if [[ $COMP_CWORD -eq 1 ]]; then
        COMPREPLY=( $(compgen -W "run list show compare trace completion" -- "$current") )
    elif [[ $command == "show" || $command == "compare" ]]; then
        COMPREPLY=( $(compgen -W "$(bourne completion --candidates "$current")" -- "$current") )
    fi
}
complete -F _bourne_completion bourne
''',
    "zsh": r'''#compdef bourne
_bourne() {
    local -a commands candidates
    commands=(run list show compare trace completion)

    if (( CURRENT == 2 )); then
        _describe 'bourne command' commands
    elif [[ $words[2] == show || $words[2] == compare ]]; then
        candidates=("${(@f)$(bourne completion --candidates "$words[CURRENT]")}")
        _describe 'experiment reference' candidates
    fi
}
compdef _bourne bourne
''',
    "fish": r'''function __bourne_experiment_references
    set -l current (commandline -ct)
    bourne completion --candidates "$current"
end

complete -c bourne -f -n 'not __fish_seen_subcommand_from run list show compare trace completion' -a 'run list show compare trace completion'
complete -c bourne -f -n '__fish_seen_subcommand_from show compare' -a '(__bourne_experiment_references)'
''',
}
