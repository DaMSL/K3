from pygments.token import Keyword, Text, Token
from pygments.lexer import RegexLexer

class K3Lexer(RegexLexer):
    name = 'K3'
    aliases = ['k3']
    filenames = ['*.k3']

    keywords = {
        'preprocessor': "include",

        "declaration": [
            "annotation", "declare", "feed", "provides", "requires", "source", "trigger"
        ],

        "type": ["bool", "int", "real", "string", "option", "ind", "collection"],

        "expression": [
            "if", "then", "else", "let", "in", "case", "of", "bind", "as", "some", "none",
        ]
    }

