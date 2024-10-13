# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Purpose
# MAGIC Automate creation of (medium) quality documentation on usages of a data source which can be refined by seres humanos.
# MAGIC
# MAGIC ## Be...
# MAGIC - Open minded about what constitues "peer reviewed research". Do not overly focus on academic research. If knowledgable industry experts are commenting on it, that's equivalent!
# MAGIC - Careful not to incorporate demagogic arguments, masquerading as research 
# MAGIC
# MAGIC ## Functions
# MAGIC For each data source...
# MAGIC - Get most cited usages (academic publishing)
# MAGIC - Get comprehensive usages (academic publishing)
# MAGIC - Get high impression research (industry "periodicals", LinkedIn top voices, X Boneheads)
# MAGIC - Get inferred industry usages (search the web to determine companies that are likely to be using the data privately, if they aren't already)
# MAGIC
# MAGIC ### GPT'ing
# MAGIC https://chatgpt.com/share/1736b6f3-9f8d-41cc-94b4-7efc0cdb5392
# MAGIC

# COMMAND ----------

from abc import ABC, abstractmethod
import requests

class CitationSleuth(ABC):
    """
    A generic Python object for automating the creation of medium-quality documentation on the usages of a specific data source.
    It browses the web or uses web APIs to find citations, high-impression industry usages, and inferred usages by companies.

    Methods:
        search_for_usages(data_source: str) -> dict:
            Abstract method to search for usages of the data source by the search string.
            Prune the results to incorporate only sources meeting high-value criteria.

        format_top_usages_as_markdown(search_results: dict) -> str:
            Abstract method to format the search results into a markdown table.

        update_object_docs(unity_catalog_object: str, markdown_table: str) -> None:
            Abstract method to update the documentation of a Unity Catalog object with the generated markdown table.
    """

    @abstractmethod
    def search_for_usages(self, data_source: str) -> dict:
        """
        Search for usages of the data source by the search string.
        
        Args:
            data_source (str): The data source to search for.
        
        Returns:
            dict: A dictionary containing pruned results that meet the high-value criteria.
        """
        pass

    @abstractmethod
    def format_top_usages_as_markdown(self, search_results: dict) -> str:
        """
        Format the search results into a markdown table.
        
        Args:
            search_results (dict): The search results to format.
        
        Returns:
            str: A markdown-formatted table of the top usages.
        """
        pass

    @classmethod
    def update_object_docs(self, unity_catalog_object: str, markdown_table: str) -> None:
        """
        Update the documentation of a Unity Catalog object with the generated markdown table.
        
        Args:
            unity_catalog_object (str): The Unity Catalog object (e.g., database, table) to update.
            markdown_table (str): The markdown-formatted table to update the documentation with.
        
        Returns:
            None
        """
        pass


# COMMAND ----------

from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup

class PubMedCitationSleuth(CitationSleuth):
    """
    A Python object that implements CitationSleuth using the PubMed API to access PubMed data.
    
    Methods:
        search_for_usages(data_source: str) -> dict:
            Searches PubMed for usages of the data source and returns pruned results.

        format_top_usages_as_markdown(search_results: dict) -> str:
            Formats the search results into a markdown table.

        update_object_docs(unity_catalog_object: str, markdown_table: str) -> None:
            Updates the documentation of a Unity Catalog object with the generated markdown table.
    """

    def search_for_usages(self, data_source: str) -> dict:
        """
        Search PubMed for usages of the data source.

        Args:
            data_source (str): The data source to search for.

        Returns:
            dict: A dictionary containing pruned results that meet the high-value criteria.
        """
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        params = {
            "db": "pubmed",
            "term": data_source,
            "retmode": "json",
            "retmax": 20
        }
        response = requests.get(base_url, params=params)
        search_results = response.json()

        article_ids = search_results.get('esearchresult', {}).get('idlist', [])
        articles = []

        for article_id in article_ids:
            article_info = self.fetch_article_info(article_id)
            if article_info:
                articles.append(article_info)

        return articles

    def fetch_article_info(self, article_id: str) -> dict:
        """
        Fetch detailed information about a specific PubMed article.

        Args:
            article_id (str): The PubMed ID of the article.

        Returns:
            dict: A dictionary containing the article's title, publication date, DOI, PMC reference count, and a link to the article.
        """
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        params = {
            "db": "pubmed",
            "id": article_id,
            "retmode": "xml"
        }
        response = requests.get(base_url, params=params)
        soup = BeautifulSoup(response.content, 'xml')
        
        title = soup.find('Item', {'Name': 'Title'}).text if soup.find('Item', {'Name': 'Title'}) else "N/A"
        publication_date = soup.find('Item', {'Name': 'EPubDate'}).text if soup.find('Item', {'Name': 'EPubDate'}) else "N/A"
        doi = soup.find('Item', {'Name': 'DOI'}).text if soup.find('Item', {'Name': 'DOI'}) else "N/A"
        pmc_ref_count = soup.find('Item', {'Name': 'PmcRefCount'}).text if soup.find('Item', {'Name': 'PmcRefCount'}) else "N/A"
        pubmed_link = f"https://pubmed.ncbi.nlm.nih.gov/{article_id}/"

        return {
            "title": title,
            "publication_date": publication_date,
            "doi": doi,
            "pmc_ref_count": pmc_ref_count,
            "pubmed_link": pubmed_link
        }

    def format_top_usages_as_markdown(self, search_results: dict) -> str:
        """
        Format the search results into a markdown table.

        Args:
            search_results (dict): The search results to format.

        Returns:
            str: A markdown-formatted table of the top usages.
        """
        markdown_table = "| Title | Publication Date | DOI | PMC Reference Count | PubMed Link |\n"
        markdown_table += "|-------|-----------------|-----|---------------------|-------------|\n"
        
        for result in search_results:
            markdown_table += f"| {result['title']} | {result['publication_date']} | {result['doi']} | {result['pmc_ref_count']} | [Link]({result['pubmed_link']}) |\n"
        
        return markdown_table

    def update_object_docs(self, unity_catalog_object: str, markdown_table: str) -> None:
        """
        Update the documentation of a Unity Catalog object with the generated markdown table.

        Args:
            unity_catalog_object (str): The Unity Catalog object (e.g., database, table) to update.
            markdown_table (str): The markdown-formatted table to update the documentation with.

        Returns:
            None
        """
        # This is a placeholder implementation. The actual implementation will depend on how the Unity Catalog documentation is managed.
        # For instance, you could write the markdown_table to a specific section in a markdown file or database entry.
        print(f"Updating documentation for {unity_catalog_object} with the following markdown table:\n")
        print(markdown_table)

# Example usage:
# sleuth = PubMedCitationSleuth()
# results = sleuth.search_for_usages("machine learning")
# markdown = sleuth.format_top_usages_as_markdown(results)
# sleuth.update_object_docs("Machine Learning Dataset", markdown)


# COMMAND ----------

sleuth = PubMedCitationSleuth()
results = sleuth.search_for_usages("medical expenditure panel survey (MEPS)")
markdown = sleuth.format_top_usages_as_markdown(results)
print(markdown)
# sleuth.update_object_docs("Machine Learning Dataset", markdown)
