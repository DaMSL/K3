from pygments.token import Keyword, Text, Token
from pygments.lexer import RegexLexer

class K3Lexer(RegexLexer):
    name = 'K3'
    aliases = ['k3']
    filenames = ['*.k3']

