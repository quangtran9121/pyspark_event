# file: scripts/generate_game_migration.py

import sys
import os
import time
from jinja2 import Environment, FileSystemLoader

def main():
    # Ví dụ gọi: python scripts/generate_game_migration.py gameC
    if len(sys.argv) < 2:
        print("Usage: python generate_game_migration.py <gameName>")
        sys.exit(1)

    game_name = sys.argv[1]  # "gameC"
    schema_name = f"{game_name}_schema"      # "gameC_schema"
    fact_table_name = "fact_purchases"       # Tên bảng fact
    revision_id = time.strftime("%Y%m%d%H%M%S") + "_auto"
    down_revision = "head"  # hoặc revision cũ

    # Tạo environment Jinja2
    template_dir = os.path.join(os.path.dirname(__file__), "..", "migrations_template")
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("create_game_schema.py.j2")

    # Render template
    rendered = template.render(
        revision_id=revision_id,
        down_revision=down_revision,
        game_name=game_name,
        schema_name=schema_name,
        fact_table_name=fact_table_name,
    )

    # Lưu file .py vào alembic/versions/
    target_file = f"../alembic/versions/{revision_id}_create_{game_name}_schema.py"
    with open(target_file, "w", encoding="utf-8") as f:
        f.write(rendered)

    print(f"[OK] Generated migration: {target_file}")
    print("Now run: alembic upgrade head")

if __name__ == "__main__":
    main()
