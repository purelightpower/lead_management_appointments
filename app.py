import re
import sys
from streamlit.web.cli import main


def run():
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    sys.exit(main())