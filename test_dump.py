import bz2
import xml.etree.ElementTree as ET
import mwparserfromhell
import json

# --- Hàm parse infobox từ wikitext ---
def parse_infobox_from_wikitext(wikitext):
    if not wikitext:
        return None
    wikicode = mwparserfromhell.parse(wikitext)
    templates = wikicode.filter_templates()
    for tpl in templates:
        name = str(tpl.name).strip().lower()
        if name.startswith("infobox"):  # bắt các template dạng {{Infobox ...}}
            info = {}
            for param in tpl.params:
                key = str(param.name).strip()
                value = str(param.value).strip()
                # loại bỏ xuống dòng, tham chiếu đơn giản
                value = value.replace("\n", " ").split("<ref")[0].strip()
                info[key] = value
            return info
    return None


# --- Hàm xử lý dump ---
def extract_infobox_from_dump(dump_path, output_path, limit=None):
    with bz2.open(dump_path, mode="rt", encoding="utf-8", errors="ignore") as f, \
         open(output_path, "w", encoding="utf-8") as out:

        context = ET.iterparse(f, events=("start", "end"))
        _, root = next(context)  # root ban đầu

        count = 0
        for event, elem in context:
            if event == "end" and elem.tag.endswith("page"):
                title_elem = elem.find("./title")
                text_elem = elem.find("./revision/text")

                if title_elem is not None and text_elem is not None:
                    title = title_elem.text
                    wikitext = text_elem.text
                    infobox = parse_infobox_from_wikitext(wikitext)
                    if infobox:
                        record = {"title": title, "infobox": infobox}
                        out.write(json.dumps(record, ensure_ascii=False) + "\n")
                        count += 1

                        if limit and count >= limit:
                            break

                elem.clear()
                root.clear()

    print(f"✅ Đã xuất {count} infobox vào {output_path}")


# --- Ví dụ chạy ---
if __name__ == "__main__":
    dump_file = "viwiki-latest-pages-articles.xml.bz2"   # dump tải từ dumps.wikimedia.org
    output_file = "infobox_viwiki.jsonl"                 # kết quả JSON lines
    extract_infobox_from_dump(dump_file, output_file, limit=1000)  # test với 1000 bài
