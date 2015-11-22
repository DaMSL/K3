-----------------------------------------------------------------------------
Generating info files.

(c) 1993-2001 Andy Gill, Simon Marlow
-----------------------------------------------------------------------------

> module Info (genInfoFile) where

> import Paths_happy            ( version )
> import LALR                   ( Lr0Item(..) )
> import GenUtils               ( str, interleave, interleave' )
> import Data.Set ( Set )
> import qualified Data.Set as Set hiding ( Set )
> import Grammar

> import Data.Array
> import Data.List (nub)
> import Data.Version           ( showVersion )

Produce a file of parser information, useful for debugging the parser.

> genInfoFile
>       :: [Set Lr0Item]
>       -> Grammar
>       -> ActionTable
>       -> GotoTable
>       -> [(Int,String)]
>       -> Array Int (Int,Int)
>       -> String
>       -> [Int]                        -- unused rules
>       -> [String]                     -- unused terminals
>       -> String

> genInfoFile items
>       (Grammar { productions = prods
>                , lookupProdNo = lookupProd
>                , lookupProdsOfName = lookupProdNos
>                , non_terminals = nonterms
>                , token_names = env
>                })
>        action goto tokens conflictArray filename unused_rules unused_terminals
>       = (showHeader
>       . showConflicts
>       . showUnused
>       . showProductions
>       . showTerminals
>       . showNonTerminals
>       . showStates
>       . showStats
>       ) ""
>   where

>   showHeader
>       = banner ("Info file generated by Happy Version " ++
>                 showVersion version ++ " from " ++ filename)

>   showConflicts
>       = str "\n"
>       . foldr (.) id (map showConflictsState (assocs conflictArray))
>       . str "\n"

>   showConflictsState (_,     (0,0)) = id
>   showConflictsState (state, (sr,rr))
>       = str "state "
>       . shows state
>       . str " contains "
>       . interleave' " and " (
>               (if sr /= 0
>                       then [ shows sr . str " shift/reduce conflicts" ]
>                       else []) ++
>                if rr /= 0
>                       then [ shows rr . str " reduce/reduce conflicts" ]
>                       else [])
>       . str ".\n"

>   showUnused =
>         (case unused_rules of
>           [] -> id
>           _  ->   interleave "\n" (
>                       map (\r ->   str "rule "
>                                  . shows r
>                                  . str " is unused")
>                               unused_rules)
>                 . str "\n")
>       . (case unused_terminals of
>           [] -> id
>           _  ->   interleave "\n" (
>                       map (\t ->   str "terminal "
>                                  . str t
>                                  . str " is unused")
>                               unused_terminals)
>                 . str "\n")

>   showProductions =
>         banner "Grammar"
>       . interleave "\n" (zipWith showProduction prods [ 0 :: Int .. ])
>       . str "\n"

>   showProduction (nt, toks, _sem, _prec) i
>       = ljuststr 50 (
>         str "\t"
>       . showName nt
>       . str " -> "
>       . interleave " " (map showName toks))
>       . str "  (" . shows i . str ")"

>   showStates =
>         banner "States"
>       . interleave "\n" (zipWith showState
>               (map Set.toAscList items) [ 0 :: Int .. ])

>   showState state n
>       = str "State ". shows n
>       . str "\n\n"
>       . interleave "\n" (map showItem [ (Lr0 r d) | (Lr0 r d) <- state, d /= 0 ])
>       . str "\n"
>       . foldr (.) id (map showAction (assocs (action ! n)))
>       . str "\n"
>       . foldr (.) id (map showGoto (assocs (goto ! n)))

>   showItem (Lr0 rule dot)
>       = ljuststr 50 (
>                 str "\t"
>               . showName nt
>               . str " -> "
>               . interleave " " (map showName beforeDot)
>               . str ". "
>               . interleave " " (map showName afterDot))
>       . str "   (rule " . shows rule . str ")"
>       where
>               (nt, toks, _sem, _prec) = lookupProd rule
>               (beforeDot, afterDot) = splitAt dot toks

>   showAction (_, LR'Fail)
>       = id
>   showAction (t, act)
>       = str "\t"
>       . showJName 15 t
>       . showAction' act
>       . str "\n"

>   showAction' LR'MustFail
>       = str "fail"
>   showAction' (LR'Shift n _)
>       = str "shift, and enter state "
>       . shows n
>   showAction' LR'Accept
>       = str "accept"
>   showAction' (LR'Reduce n _)
>       = str "reduce using rule "
>       . shows n
>   showAction' (LR'Multiple as a)
>       = showAction' a
>       . str "\n"
>       . interleave "\n"
>               (map (\a' -> str "\t\t\t(" . showAction' a' . str ")")
>                (nub (filter (/= a) as)))
>   showAction' LR'Fail = error "showAction' LR'Fail: Unhandled case"

>   showGoto (_, NoGoto)
>       = id
>   showGoto (nt, Goto n)
>       = str "\t"
>       . showJName 15 nt
>       . str "goto state "
>       . shows n
>       . str "\n"

>   showTerminals
>       = banner "Terminals"
>       . interleave "\n" (map showTerminal tokens)
>       . str "\n"

>   showTerminal (t,s)
>       = str "\t"
>       . showJName 15 t
>       . str "{ " . str s . str " }"

>   showNonTerminals
>       = banner "Non-terminals"
>       . interleave "\n" (map showNonTerminal nonterms)
>       . str "\n"

>   showNonTerminal nt
>       = str "\t"
>       . showJName 15 nt
>       . (if (length nt_rules == 1)
>               then str " rule  "
>               else str " rules ")
>       . foldr1 (\a b -> a . str ", " . b) nt_rules
>       where nt_rules = map shows (lookupProdNos nt)

>   showStats
>       = banner "Grammar Totals"
>       . str   "Number of rules: " . shows (length prods)
>       . str "\nNumber of terminals: " . shows (length tokens)
>       . str "\nNumber of non-terminals: " . shows (length nonterms)
>       . str "\nNumber of states: " . shows (length items)
>       . str "\n"

>   nameOf n    = env ! n
>   showName    = str . nameOf
>   showJName j = str . ljustify j . nameOf

> ljustify :: Int -> String -> String
> ljustify n s = s ++ replicate (max 0 (n - length s)) ' '

> ljuststr :: Int -> (String -> String) -> String -> String
> ljuststr n s = str (ljustify n (s ""))

> banner :: String -> String -> String
> banner s
>       = str "-----------------------------------------------------------------------------\n"
>       . str s
>       . str "\n-----------------------------------------------------------------------------\n"

