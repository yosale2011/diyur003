# ניהול משמרות מקומי

אפליקציית FastAPI לניהול משמרות.

## הרצה מקומית

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload
```

או להרצת האפליקציה ישירות:

```bash
python app.py
```

## העלאה לאינטרנט

### אפשרות 1: Railway (מומלץ - הכי קל)

1. הירשם ל-[Railway](https://railway.app)
2. לחץ על "New Project"
3. בחר "Deploy from GitHub repo" (או העלה את הקבצים ישירות)
4. Railway יזהה אוטומטית את ה-Procfile ויריץ את האפליקציה
5. האתר יהיה זמין בכתובת שנוצרה אוטומטית

**חשוב:** ודא ש-`database.db` נשמר. Railway מספק אחסון מתמשך.

### אפשרות 2: Render

1. הירשם ל-[Render](https://render.com)
2. לחץ על "New +" → "Web Service"
3. חבר את ה-repository שלך (GitHub/GitLab)
4. הגדר:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python -m uvicorn app:app --host 0.0.0.0 --port 8000`
5. לחץ על "Create Web Service"

### אפשרות 3: Fly.io

1. התקן את [flyctl](https://fly.io/docs/getting-started/installing-flyctl/)
2. הרץ: `fly launch`
3. עקוב אחר ההוראות

## הערות

- האפליקציה משתמשת ב-SQLite. ודא שהבסיס נתונים נשמר בפלטפורמה שבה אתה משתמש.
- ניתן להגדיר את מיקום מסד הנתונים באמצעות משתנה סביבה `DATABASE_PATH`. ברירת המחדל היא `./database.db` (בתיקיית הפרויקט).
- עבור ייצור, שקול לעבור ל-PostgreSQL או MySQL.
- ודא שהפורט מוגדר דרך משתנה סביבה `PORT` (Railway/Render עושים זאת אוטומטית).


# diyur003
