import time
import pandas as pd
import numpy as np
import re
import logging
import requests
import unicodedata
from difflib import SequenceMatcher
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import customtkinter as ctk
from tkinter import filedialog, messagebox
import concurrent.futures
from threading import Event, Thread, Lock

# --- Cấu hình logging & biến toàn cục ---
data = None  # Biến toàn cục để lưu DataFrame
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
pause_event = Event()
pause_event.set()  # Cho phép chạy ban đầu
processed_count = 0    # Số dòng đã xử lý
total_count = 0        # Tổng số dòng cần xử lý
progress_lock = Lock() # Khóa cập nhật tiến trình
CHROMEDRIVER_PATH = r"C:\Users\Khanh\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
EXTENSION_PATH = r"C:\Users\Khanh\OneDrive\Desktop\machinelearning"

# --- Danh sách từ viết tắt (định nghĩa toàn cục) ---
abbreviation_map = {
    r"\bcty\b": "cong ty", r"\bcp\b": "co phan", r"\btnhh\b": "trach nhiem huu han",
    r"\bcn\b": "chi nhanh", r"\bxd\b": "xay dung", r"\btm\b": "thuong mai",
    r"\bvl\b": "vat lieu", r"\bjesco\b": "jsc", r"\bpccc\b": "phong chay chua chay",
    r"\bdv\b": "dich vu", r"\bsx\b": "san xuat", r"\bvlxd\b": "vat lieu xay dung",
    r"\btmdv\b": "thuong mai dich vu", r"\bmtv\b": "mot thanh vien",
    r"\bvt\b": "van tai", r"\bttnt\b": "khong xac dinh", r"\btmxd\b": "thuong mai xay dung",
    r"\bxnk\b": "xuat nhap khau", r"\bdntn\b": "doanh nghiep tu nhan",
    r"\bsxtm\b": "san xuat thuong mai", r"\bunc\b": "universal network connection",
    r"\bhtx\b": "hop tac xa", r"\bdvth\b": "dich vu thuong mai",
    r"\btnhh mtv\b": "trach nhiem huu han mot thanh vien", r"\bcty cp\b": "cong ty co phan",
    r"\bcty tnhh\b": "cong ty trach nhiem huu han", r"\btm & dv\b": "thuong mai va dich vu",
    r"\bsx-tm\b": "san xuat - thuong mai", r"\btm dv\b": "thuong mai dich vu",
    r"\bsx tm dv\b": "san xuat thuong mai dich vu", r"\btm dv xd\b": "thuong mai dich vu xay dung",
    r"\bcty tnhh xd\b": "cong ty trach nhiem huu han xay dung", r"\bcty co phan\b": "cong ty co phan",
    r"\btm-dv-xd\b": "thuong mai - dich vu - xay dung", r"\btm-dv\b": "thuong mai - dich vu",
    r"\btm - dv\b": "thuong mai - dich vu", r"\btnhh mot thanh vien\b": "trach nhiem huu han mot thanh vien",
    r"\bxd tm\b": "xay dung thuong mai", r"\btm dv sx\b": "thuong mai dich vu san xuat",
    r"\btnhh sx tm dv\b": "trach nhiem huu han san xuat thuong mai dich vu",
    r"\bsx & dv\b": "san xuat va dich vu", r"\bsx-tm-dv\b": "san xuat - thuong mai - dich vu",
    r"\btm-sx-dv\b": "thuong mai - san xuat - dich vu", r"\bdv bv\b": "dich vu bao ve"
}
abbreviation_map = {re.compile(k): v for k, v in abbreviation_map.items()}

# --- Thiết lập ChromeDriver ---
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    #chrome_options.add_argument(f'--load-extension={EXTENSION_PATH}')
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    service = Service(CHROMEDRIVER_PATH)
    return webdriver.Chrome(service=service, options=chrome_options)

# --- Hàm trích xuất MST từ văn bản ---
def extract_tax_id(text, tax_prefix):
    if tax_prefix:
        num_digits = 10 - len(tax_prefix)
        pattern = rf'\b{tax_prefix}\d{{{num_digits}}}(?:-\d{{3}})?\b'
    else:
        pattern = r'\b\d{10}(?:-\d{3})?\b'
    matches = re.findall(pattern, text)
    return matches  # Trả về tất cả MST tìm thấy

# --- Hàm tìm kiếm tất cả candidate MST từ kết quả Bing ---
def search_tax_info(driver, search_query, tax_prefix, search_engine="Bing"):
    candidate_tax_ids = []
    try:
        driver.get("https://www.bing.com")
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )
        driver.execute_script("arguments[0].value = arguments[1];", search_box, search_query)
        search_box.send_keys(Keys.RETURN)
        results = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.b_algo"))
        )
        for result in results:
            tax_ids = extract_tax_id(result.text, tax_prefix)
            for tax_id in tax_ids:
                if tax_id not in candidate_tax_ids:
                    candidate_tax_ids.append(tax_id)
    except Exception as e:
        logging.error(f"Lỗi khi tìm kiếm trên Bing: {e}")
    return candidate_tax_ids

# --- API tra cứu thông tin công ty từ MST ---
API_URL_TEMPLATE = "https://api.vietqr.io/v2/business/{taxCode}"
def api_lookup(tax_id):
    url = API_URL_TEMPLATE.replace("{taxCode}", tax_id)
    max_retries = 15
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == "00":
                    data = result.get("data", {})
                    company_name = data.get("name", "Không có thông tin")
                    address = data.get("address", "Không có thông tin")
                    return company_name, address
        except Exception:
            pass
        time.sleep(1)
    return "Error: Retry exhausted", "Error"

# --- Hàm normalize_text và check_similarity ---
def normalize_text(text):
    if text is None:
        return ""
    text = unicodedata.normalize("NFD", str(text))
    text = text.encode("ascii", "ignore").decode("utf-8").lower()
    for pattern, replacement in abbreviation_map.items():
        text = pattern.sub(replacement, text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def check_similarity(text1, text2):
    normalized1 = normalize_text(text1)
    normalized2 = normalize_text(text2)
    return SequenceMatcher(None, normalized1, normalized2).ratio()

# --- Hàm xác thực candidate MST qua API ---
def validate_tax_candidates(candidate_tax_ids, expected_company, province):
    normalized_province = normalize_text(province)
    for tax_id in candidate_tax_ids:
        api_company, api_address = api_lookup(tax_id)
        similarity = check_similarity(api_company, expected_company)
        if similarity > 0.9:  # Tương đồng > 90%
            normalized_address = normalize_text(api_address)
            if normalized_province in normalized_address:  # Cùng tỉnh
                return tax_id, "Cùng tỉnh"
    return None, None

# --- Xử lý dữ liệu từng chunk (song song) ---
def process_chunk(chunk_data, keyword, tax_prefix, search_engine, province):
    global processed_count, data
    driver = setup_driver()
    for index, row in chunk_data.iterrows():
        pause_event.wait()
        company_name = str(row.get('Tên công ty', '')).strip()
        district = str(row.get('Quận', '')).strip()
        if not company_name:
            with progress_lock:
                data.at[index, 'MST'] = "Không có tên công ty"
                data.at[index, 'Kết quả tỉnh'] = ""
        else:
            search_query = ' '.join([part for part in [company_name, district, keyword] if part])
            logging.info(f"Tìm kiếm (index {index}) trên Bing: {search_query}")
            candidate_tax_ids = search_tax_info(driver, search_query, tax_prefix, search_engine)
            if candidate_tax_ids:
                valid_tax, province_status = validate_tax_candidates(candidate_tax_ids, company_name, province)
                with progress_lock:
                    data.at[index, 'MST'] = valid_tax if valid_tax else "Không tìm thấy"
                    data.at[index, 'Kết quả tỉnh'] = province_status if valid_tax else ""
            else:
                with progress_lock:
                    data.at[index, 'MST'] = "Không tìm thấy"
                    data.at[index, 'Kết quả tỉnh'] = ""
        with progress_lock:
            processed_count += 1
    driver.quit()

# --- Xử lý dữ liệu đồng thời và cập nhật file Excel ---
def process_data_concurrent(input_file, keyword, tax_prefix, num_threads, search_engine, province):
    global data, total_count, processed_count
    processed_count = 0
    try:
        data = pd.read_excel(input_file)
    except Exception as e:
        logging.error(f"Lỗi khi đọc file Excel: {e}")
        messagebox.showerror("Lỗi", "Không thể đọc file Excel!")
        return

    if 'Tên công ty' not in data.columns:
        messagebox.showerror("Lỗi", "File Excel cần có cột 'Tên công ty'.")
        return
    if 'Quận' not in data.columns:
        data['Quận'] = ''
    if 'MST' not in data.columns:
        data['MST'] = ''
    if 'Kết quả tỉnh' not in data.columns:
        data['Kết quả tỉnh'] = ''

    total_count = len(data)
    chunks = np.array_split(data, num_threads)
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_chunk, chunk, keyword, tax_prefix, search_engine, province) for chunk in chunks]
        concurrent.futures.wait(futures)

    try:
        data.to_excel(input_file, index=False)
        logging.info(f"Kết quả đã được lưu vào file: {input_file}")
        messagebox.showinfo("Hoàn tất", f"Kết quả đã được lưu vào file: {input_file}")
    except Exception as e:
        logging.error(f"Lỗi khi ghi file Excel: {e}")
        messagebox.showerror("Lỗi", "Không thể ghi file Excel!")

# --- Cập nhật tiến trình trên giao diện ---
def update_gui_progress():
    progress_label.configure(text=f"Đã xử lý: {processed_count}/{total_count}")
    progress_bar.set(processed_count / total_count if total_count else 0)
    root.after(500, update_gui_progress)

# --- Hàm chọn file ---
def select_file(save=False):
    if save:
        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
    else:
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])
    if file_path:
        input_file_entry.delete(0, ctk.END)
        input_file_entry.insert(0, file_path)

# --- Các hàm xử lý nút bấm ---
def start_processing():
    input_file = input_file_entry.get()
    keyword = keyword_entry.get()
    tax_prefix = tax_prefix_entry.get()
    search_engine = "Bing"  # Hardcode chỉ sử dụng Bing
    province = province_entry.get()
    try:
        n_threads = int(num_threads_entry.get())
    except ValueError:
        messagebox.showerror("Lỗi", "Số luồng phải là một số nguyên!")
        return
    if not all([input_file, keyword, province]):
        messagebox.showerror("Lỗi", "Vui lòng nhập đầy đủ thông tin!")
        return
    Thread(target=process_data_concurrent, args=(input_file, keyword, tax_prefix, n_threads, search_engine, province), daemon=True).start()

def pause_processing():
    pause_event.clear()
    logging.info("Tạm dừng xử lý dữ liệu.")

def continue_processing():
    pause_event.set()
    logging.info("Tiếp tục xử lý dữ liệu.")

def save_current_file():
    global data
    if data is None:
        messagebox.showerror("Lỗi", "Không có dữ liệu để lưu!")
        return
    file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
    if file_path:
        try:
            with progress_lock:
                data.to_excel(file_path, index=False)
            logging.info(f"File đã được lưu tại: {file_path}")
            messagebox.showinfo("Thông báo", f"File đã được lưu thành công tại: {file_path}")
        except Exception as e:
            logging.error(f"Lỗi khi lưu file: {e}")
            messagebox.showerror("Lỗi", "Không thể lưu file!")

# --- Cấu hình CustomTkinter ---
ctk.set_appearance_mode("System")
ctk.set_default_color_theme("blue")

# --- Tạo cửa sổ chính ---
root = ctk.CTk()
root.title("Ứng dụng tìm mã số thuế và xác thực tên công ty")
root.geometry("800x600")

# --- Tạo Frame để bố trí các widget ---
input_frame = ctk.CTkFrame(master=root)
input_frame.pack(padx=20, pady=20, fill="both", expand=True)

# Row 0: Chọn file Excel
input_file_label = ctk.CTkLabel(master=input_frame, text="Chọn file Excel đầu vào:")
input_file_label.grid(row=0, column=0, padx=10, pady=10, sticky="w")
input_file_entry = ctk.CTkEntry(master=input_frame, width=400)
input_file_entry.grid(row=0, column=1, padx=10, pady=10)
input_file_button = ctk.CTkButton(master=input_frame, text="Chọn file", command=lambda: select_file(False))
input_file_button.grid(row=0, column=2, padx=10, pady=10)

# Row 1: Nhập từ khóa
keyword_label = ctk.CTkLabel(master=input_frame, text="Nhập từ khóa tìm kiếm:")
keyword_label.grid(row=1, column=0, padx=10, pady=10, sticky="w")
keyword_entry = ctk.CTkEntry(master=input_frame, width=400)
keyword_entry.grid(row=1, column=1, padx=10, pady=10, columnspan=2)

# Row 2: Nhập đầu số MST
tax_prefix_label = ctk.CTkLabel(master=input_frame, text="Nhập đầu số mã số thuế:")
tax_prefix_label.grid(row=2, column=0, padx=10, pady=10, sticky="w")
tax_prefix_entry = ctk.CTkEntry(master=input_frame, width=400)
tax_prefix_entry.grid(row=2, column=1, padx=10, pady=10, columnspan=2)

# Row 3: Nhập Tỉnh/TP cần kiểm tra
province_label = ctk.CTkLabel(master=input_frame, text="Tỉnh/TP cần kiểm tra:")
province_label.grid(row=3, column=0, padx=10, pady=10, sticky="w")
province_entry = ctk.CTkEntry(master=input_frame, width=400)
province_entry.grid(row=3, column=1, padx=10, pady=10, columnspan=2)

# Row 4: Số luồng xử lý
num_threads_label = ctk.CTkLabel(master=input_frame, text="Số luồng xử lý:")
num_threads_label.grid(row=4, column=0, padx=10, pady=10, sticky="w")
num_threads_entry = ctk.CTkEntry(master=input_frame, width=400)
num_threads_entry.grid(row=4, column=1, padx=10, pady=10, columnspan=2)
num_threads_entry.insert(0, "1")

# Row 5: Các nút điều khiển
start_button = ctk.CTkButton(master=input_frame, text="Bắt đầu", command=start_processing)
start_button.grid(row=5, column=0, padx=10, pady=10)
pause_button = ctk.CTkButton(master=input_frame, text="Tạm dừng", command=pause_processing)
pause_button.grid(row=5, column=1, padx=10, pady=10)
continue_button = ctk.CTkButton(master=input_frame, text="Tiếp tục", command=continue_processing)
continue_button.grid(row=5, column=2, padx=10, pady=10)
save_button = ctk.CTkButton(master=input_frame, text="Lưu file hiện tại", command=save_current_file)
save_button.grid(row=5, column=3, padx=10, pady=10)

# Row 6: Hiển thị tiến trình
progress_label = ctk.CTkLabel(master=input_frame, text="Chưa bắt đầu")
progress_label.grid(row=6, column=0, padx=10, pady=10, sticky="w")
progress_bar = ctk.CTkProgressBar(master=input_frame, width=400)
progress_bar.grid(row=6, column=1, padx=10, pady=10, columnspan=2)

root.after(500, update_gui_progress)
root.mainloop()