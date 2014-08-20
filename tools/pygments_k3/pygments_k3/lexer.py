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

    tokens = {
        'root': [
            (r'//.*?$', Token.Comment),
            (r'\b(' + r'|'.join(keywords["preprocessor"]) + r')\s*\b(?!\.)', Keyword.Namespace),
            (r'\b(' + r'|'.join(keywords["declaration"]) + r')\s*\b(?!\.)', Keyword.Declaration),
            (r'\b(' + r'|'.join(keywords["type"]) + r')\s*\b(?!\.)', Keyword.Type),
            (r'\b(' + r'|'.join(keywords["expression"]) + r')\s*\b(?!\.)', Keyword.Reserved),
            (r'.*\n', Text),
        ],
    }
