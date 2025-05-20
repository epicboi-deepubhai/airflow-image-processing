from PIL import Image
import logging
import requests
from io import BytesIO

logger = logging.getLogger(__name__)


def resize_image(image_path, max_size=500):
    logger.info(f"Resizing image: {image_path}")

    image = Image.open(image_path)
    width, height = image.size

    if width > height:
        if width > max_size:
            new_width = max_size
            new_height = int((height / width) * new_width)
    else:
        if height > max_size:
            new_height = max_size
            new_width = int((width / height) * new_height)

    if width > max_size or height > max_size:
        resized_image = image.resize((new_width, new_height), Image.LANCZOS)

        if resized_image.mode == "RGBA":
            resized_image = resized_image.convert("RGB")

        resized_image.save(image_path)
        logger.info(f"Image resized to {new_width}x{new_height}")
        return True

    logger.info("Image doesn't need resizing")
    return False


def add_watermark(
    image_path, logo_url="https://static.thenounproject.com/png/1546105-200.png"
):
    logger.info(f"Adding watermark to image: {image_path}")

    try:
        response = requests.get(logo_url, stream=True)
        logo = Image.open(BytesIO(response.content))

        image = Image.open(image_path)
        image_width, image_height = image.size

        shorter_side = min(image_width, image_height)
        new_logo_width = int(shorter_side * 0.2)
        logo_aspect_ratio = logo.width / logo.height
        new_logo_height = int(new_logo_width / logo_aspect_ratio)

        logo = logo.resize((new_logo_width, new_logo_height), Image.LANCZOS)

        paste_x, paste_y = image_width - new_logo_width, image_height - new_logo_height

        try:
            image.paste(logo, (paste_x, paste_y), logo)
        except Exception as e:
            image.paste(logo, (paste_x, paste_y))
            logger.warning(f"Watermark added without transparency: {str(e)}")

        if image.mode == "RGBA":
            image = image.convert("RGB")

        image.save(image_path)
        logger.info("Added watermark successfully")
        return True

    except Exception as e:
        logger.error(f"Error adding watermark: {str(e)}")
        raise e
