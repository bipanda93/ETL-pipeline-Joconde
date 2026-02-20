from dotenv import load_dotenv
import logging


load_dotenv()

logging.basicConfig(
  filename="Etl.log",
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s"
)


logging.INFO("Début du traitement")


# logging  -> une erreur s'est produite


try:
  raise ValueError("Erreur !")
except Exception as e:
  logging.exception(" une erreur s'est produite")