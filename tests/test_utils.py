import pytest
import os
import zipfile
from modules.utils import extract_site_id, create_readme_file, create_zip_report

def test_extract_site_id():
    # Test cases with UA and UB followed by digits
    assert extract_site_id("This is UA123 from topic") == "UA123"
    assert extract_site_id("UB456 is here") == "UB456"
    assert extract_site_id("Mixed UB001 text") == "UB001"

    # Test cases where no ID match is present
    assert extract_site_id("без айди топик") == "topic_без айди топик"
    assert extract_site_id("Just text") == "topic_Just text"

    # Test cases with VDO
    assert extract_site_id("VDO 456") == "topic_VDO 456"

    # Test empty and None-like behavior (function assumes str input)
    assert extract_site_id("") == "unknown"

def test_create_readme_file(tmp_path):
    topic_dir = tmp_path / "test_topic"
    topic_dir.mkdir()
    
    equipment_lists = {
        'demontaj': ['Switch 1', 'Router 2'],
        'montaj': ['Switch 3']
    }
    text_messages = ["Worker message 1\n", "Worker message 2"]
    
    readme_path = create_readme_file(str(topic_dir), equipment_lists, text_messages)
    
    assert os.path.exists(readme_path)
    with open(readme_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    assert "ОБОРУДОВАНИЕ НА ДЕМОНТАЖ:" in content
    assert "- Switch 1" in content
    assert "ОБОРУДОВАНИЕ НА МОНТАЖ:" in content
    assert "- Switch 3" in content
    assert "Worker message 1" in content

def test_create_zip_report(tmp_path):
    source_folder = tmp_path / "source"
    output_folder = tmp_path / "output"
    source_folder.mkdir()
    output_folder.mkdir()
    
    # Create mock Demontaj folder and file
    demontaj_dir = source_folder / "Demontaj"
    demontaj_dir.mkdir()
    (demontaj_dir / "photo1.jpg").write_text("mock photo content")
    
    # Create mock README
    (source_folder / "README.txt").write_text("mock readme content")
    
    zip_path = create_zip_report("UA123", str(source_folder), str(output_folder), "UA123 Обслуговування ВДО")
    
    assert os.path.exists(zip_path)
    assert zip_path.endswith("UA123_VDO.zip")
    
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        namelist = zipf.namelist()
        # Because we copy Demontaj and README to a temp folder and zip that,
        # the contents should include Demontaj/photo1.jpg and README.txt directly
        assert "README.txt" in namelist
        assert set(namelist) >= {"README.txt", "Demontaj/photo1.jpg"}
