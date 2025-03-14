
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_, inspect, select, func
from nltk import download, word_tokenize
from nltk.data import find
from nltk.stem import PorterStemmer
from rich import print

from .db import Base, Paper
from .build_db import build_db, DB_PATH
from .utils import new_logger
import argparse



engine = sqlalchemy.create_engine(f'sqlite:///{str(DB_PATH)}')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

logger = new_logger("Top4Grep")
stemmer = PorterStemmer()

CONFERENCES = ["NDSS", "IEEE S&P", "USENIX", "CCS", "ASE", "ICSE", "FSE", "ISSTA"]

def validate_db():
    assert DB_PATH.exists(), f"Database file does not exist at {DB_PATH}. Need to build a paper database first to perform wanted queries."


    inspector = inspect(engine)
    assert inspector.has_table('Paper'), "Need to build paper database."

    # Check if the 'Paper' table has entries
    with engine.connect() as connection:
        result = connection.execute(select(func.count()).select_from(sqlalchemy.table('Paper')))
        count = result.scalar()
        assert count > 0, "DB was cleared, build paper database again."

def del_db():
    Base.metadata.drop_all(engine)
    engine.dispose()

# Function to check and download 'punkt' if not already available
def check_and_download_punkt():
    try:
        # Check if 'punkt' is available, this will raise a LookupError if not found
        find('tokenizers/punkt_tab')
        #print("'punkt' tokenizer models are already installed.")
    except LookupError:
        print("'punkt' tokenizer models not found. Downloading...")
        # Download 'punkt' tokenizer models
        download('punkt_tab')
        
# trim word tokens from tokenizer to stem i.e. exploiting to exploit
def fuzzy_match(title):
    tokens = word_tokenize(title)
    return [stemmer.stem(token) for token in tokens]

def existed_in_tokens(tokens, keywords):
    return all(map(lambda k: stemmer.stem(k.lower()) in tokens, keywords))

def grep(keywords, abstract):
    # TODO: currently we only grep either from title or from abstract, also grep from other fields in the future maybe?
    keywords_list = [x for sublist in keywords for x in sublist]
    and_groups = [and_(*[Paper.title.contains(x) for x in sublist]) for sublist in keywords_list]
    constraints = or_(*and_groups)
    with Session() as session:
        papers = session.query(Paper).filter(constraints).all()
    #check whether whether nltk tokenizer data is downloaded
    check_and_download_punkt()
    #tokenize the title and filter out the substring matches
    filter_paper = []
    paper_titles = set()
    for paper in papers:
        for keyword in keywords:
            if all([stemmer.stem(x.lower()) in fuzzy_match(paper.title.lower()) for x in keyword]):
                if paper.title not in paper_titles:
                    filter_paper.append(paper)
                    paper_titles.add(paper.title)
    if abstract:
        and_groups = [and_(*[Paper.abstract.contains(x) for x in sublist]) for sublist in keywords_list]
        constraints = or_(*and_groups)
        with Session() as session:
            papers = session.query(Paper).filter(constraints).all()
        #check whether whether nltk tokenizer data is downloaded
        check_and_download_punkt()
        for paper in papers:
            for keyword in keywords:
                if all([stemmer.stem(x.lower()) in fuzzy_match(paper.abstract.lower()) for x in keyword]):
                    if paper.title not in paper_titles:
                        filter_paper.append(paper)
                        paper_titles.add(paper.title)
        
    # perform customized sorthing
    papers = sorted(filter_paper, key=lambda paper: paper.year + CONFERENCES.index(paper.conference)/10, reverse=True)
    return papers


def show_papers(papers):
    for paper in papers:
        print(f"[link={paper.url}]{paper}[/link] ({paper.url})")


def main():
    parser = argparse.ArgumentParser(description='Scripts to query the paper database',
                                     usage="%(prog)s [options] -k <keywords>")
    parser.add_argument('-k', type=str, help="keywords to grep, separated by ','. For example, 'linux,kernel,exploit'", default='')
    parser.add_argument('--build-db', action="store_true", help="Builds the database of conference papers")
    parser.add_argument('--abstract', action="store_true", help="Involve abstract into the database's building or query (Need Chrome for building)")
    parser.add_argument('--clear-db', action="store_true", help="Clears the database of conference papers")
    parser.add_argument('--years', type=int, help="number of years to go back by, ex 10", default=None)
    parser.add_argument('--exclude_software', action="store_true", help="Exclude software papers in the database")
    args = parser.parse_args()

    if args.k:
        validate_db()
        # this should split up everything like 'linux,kernel,exploit' into ['linux', 'kernel', 'exploit']
        # or everything like 'linux,kernel|exploit' into [['linux' 'kernel'], ['exploit']]
        or_keywords = [x.strip().lower() for x in args.k.split('|')]
        keywords = []
        for or_keyword in or_keywords:
            and_keywords = or_keyword.split(',')
            keywords.append(and_keywords)

        if keywords:
            # flatten the list of lists and account for the use of or vs and
            keywords_list = [x for sublist in keywords for x in sublist]
            logger.info("Grep based on the following keywords: %s", ', '.join(keywords_list))
        else:
            logger.warning("No keyword is provided. Return all the papers.")

        papers = grep(keywords, args.abstract)
        logger.debug(f"Found {len(papers)} papers")

        show_papers(papers)
    elif args.build_db:
        print("Building db...")
        include_software = not args.exclude_software
        if args.years:
            print(f"Going back {args.years} years")
        if args.exclude_software:
            print("Excluding software papers")

        build_db(args.abstract, include_software, args.years)
    elif args.clear_db:
        print("Clearing db...")
        del_db()


if __name__ == "__main__":
    main()
