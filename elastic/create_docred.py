from elasticsearch import Elasticsearch
from transformers import AutoTokenizer
from collections import Counter, defaultdict
from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List, Dict



es = Elasticsearch("http://localhost:9200")

# Load model once
model = SentenceTransformer('m3hrdadfi/roberta-zwnj-wnli-mean-tokens')
tokenizer = AutoTokenizer.from_pretrained("HooshvareLab/bert-fa-zwnj-base")
property_set = {'P6': 'رئیسجمهور یا شهردار', 'P17': 'کشور', 'P19': 'زادگاه', 'P20': 'محل مرگ', 'P22': 'پدرش', 'P25': 'مادرش', 'P26': 'همسر', 'P27': 'تبعۀ', 'P30': 'قاره', 'P31': 'نمونهای از', 'P35': 'حاکم', 'P36': 'پایتخت', 'P37': 'زبان رسمی', 'P39': 'منصب', 'P40': 'فرزند', 'P50': 'نویسنده', 'P54': 'عضو تیم ورزشی', 'P57': 'کارگردان', 'P58': 'فیلمنامهنویس', 'P69': 'دانشآموختهٔ', 'P86': 'آهنگساز', 'P102': 'عضو حزب', 'P108': 'کار میکند/میکرد در', 'P112': 'بنیانگذار', 'P118': 'لیگ', 'P123': 'ناشر', 'P127': 'دارنده', 'P131': 'موقعیت در تقسیمات کشوری', 'P136': 'سبک', 'P137': 'کارفرما', 'P140': 'دین', 'P150': 'زیربخش', 'P155': 'پس از', 'P156': 'پیش از', 'P159': 'محل ستاد', 'P161': 'با هنرمندی', 'P162': 'تهیهکننده', 'P166': 'جوایز', 'P170': 'پدیدآورنده', 'P171': 'آرایهٔ والد', 'P172': 'قوم', 'P175': 'اجرا کننده', 'P176': 'سازنده', 'P178': 'توسعهدهنده', 'P179': 'دنباله', 'P190': 'شهرهای خواهرخوانده', 'P194': 'مجلس', 'P205': 'کشورهای ساحلی دریاچه', 'P206': 'آب سطحی پیرامون', 'P241': 'شاخهٔ نظامی', 'P264': 'برچسب ضبط', 'P272': 'شرکتهای رسانه', 'P276': 'مکان کنونی', 'P279': 'زیرردۀ', 'P355': 'شرکتهای زیرمجموعه', 'P361': 'جزئی از', 'P364': 'زبان اصلی', 'P400': 'سکو', 'P403': 'میریزد به', 'P449': 'پخش شده از شبکه یا کانالِ', 'P463': 'از اعضای', 'P488': 'مدیر مسئول', 'P495': 'زادگاه فرهنگی', 'P527': 'دربرگیرنده', 'P551': 'جای زندگی', 'P569': 'زادروز', 'P570': 'درگذشت', 'P571': 'تاریخ ساخت\u202f/\u2009تأسیس', 'P576': 'تاریخ فروپاشی', 'P577': 'تاریخ انتشار', 'P580': 'زمان آغاز', 'P582': 'زمان پایان', 'P585': 'زمان رویداد', 'P607': 'جنگها', 'P674': 'شخصیتها', 'P676': 'سرایندۀ اشعار', 'P706': 'موقعیت', 'P710': 'با حضورِ', 'P737': 'متأثر از', 'P740': 'مکان شکلگیری', 'P749': 'نهاد ارشد', 'P800': 'اثر (ها)', 'P807': 'جداشده از', 'P840': 'مکان وقوع داستان', 'P937': 'محل انجام کار', 'P1001': 'متعلق به حوزهٔ قضایی', 'P1056': 'تولیدکنندهٔ', 'P1198': 'نرخ بیکاری', 'P1336': 'ادعا کننده مالکیت', 'P1344': 'شرکت داشته است در', 'P1365': 'به جای', 'P1366': 'جایگزینشده توسط', 'P1376': 'پایتخت است برای', 'P1412': 'زبانهای شخص', 'P1441': 'اشاره شده در کتابِ', 'P3373': 'همنیا'}
property_description = {'P6': 'رئیس جمهور، استاندار، شهردار، دهدار', 'P17': 'سرزمین', 'P19': 'جایی که فرد متولد شده است (ریزترین جای در دسترس را بنویسید مثلا شهر به جای کشور)', 'P20': 'باید تا حد امکان دقیق و ریزتر باشد (مثلا شهر درگذشت نه کشور درگذشت)', 'P22': 'پدر این شخص', 'P25': 'مادر فرد مورد نظر', 'P26': 'همسر یا همبستر شخص موردنظر', 'P27': 'کشوری که شخص مورد نظر را به عنوان شهروند به رسمیت می\u200cشناسد', 'P30': 'قاره\u200cای که آیتم در آن است', 'P31': 'آیتم مورد نظر یک نوع ... است', 'P35': '', 'P36': 'مرکز حکومتی کشور یا ایالت یا سایر تقسیمات مدیریتی', 'P37': '', 'P39': 'منصب یا پست کنونی یا سابق آیتم', 'P40': 'فرزند(ان) شخص', 'P50': 'پدیدآورندهٔ اصلی یک اثر نوشتاری', 'P54': 'تیم یا باشگاه ورزشی که فرد مورد نظر در آن هست یا بوده است', 'P57': 'کارگردان فیلم، بازی ویدئویی یا موارد مشابه', 'P58': 'نویسندهٔ فیلم\u200cنامه', 'P69': 'آموزشگاهی که آیتم در آن تحصیل کرده است (نام دانشگاه یا مؤسسۀ آموزشی باید ذکر شود نه نام رشتۀ تحصیلی)', 'P86': 'شخصی که آهنگ را نوشته است', 'P102': 'حزب سیاسی که این سیاستمدار عضو آن است یا زمانی بوده است', 'P108': 'سازمانی که شخص برای آن کار می\u200cکند یا کار کرده است', 'P112': 'بنیان\u200cگذار یا بنیان\u200cگذاران مکان یا سازمان مورد نظر', 'P118': 'لیگی که تیم در آن بازی می\u200cکند', 'P123': 'سازمان انتشاراتی که اثر را منتشر کرده است', 'P127': 'صاحب حقوقی موضوع', 'P131': 'جایگاه آیتم مورد نظر در تقسیمات کشوری', 'P136': 'ژانر آثار خلاقه یا سبک کار یک هنرمند (فعلا برای موسیقی و موسیقی دانان به کار نبرید)', 'P137': '', 'P140': 'دین شخص (باید توسط خود وی یا منابع تاریخی گفته شده باشد)', 'P150': 'زیربخش مستقیم یک بخش اداری', 'P155': 'اثری که قبل از این اثر پدید آمده است.', 'P156': 'نام کار بعدی (در مجموعه های چندگانهٔ فیلم یا کتاب یا مجموعه تلویزیونی)', 'P159': 'محل دقیق دفتر مرکزی سازمان مورد نظر', 'P161': 'هنرمندانی که در یک فیلم نامشان با حروف درشت روی پوستر می آید (برای تعیین نقش اول یا نقش\u200cهای مرتبۀ پایین از گستره\u200cنمای P453 استفاده کنید)', 'P162': 'تهیه\u200cکنندهٔ این فیلم یا ترانه', 'P166': 'جوایزی که به فرد، سازمان، یا اثر داده شده است', 'P170': 'شخصی که اثر را پدید آورده است (هنگامی استفاده شود که خاصیت دقیق\u200cتری موجود نباشد)', 'P171': 'نزدیک\u200cترین آرایهٔ والد آرایهٔ مورد بحث', 'P172': 'قومیت آیتم', 'P175': 'بازیگر ، نوازنده ، گروه یا مجری دیگری که با این نقش یا اثر موسیقی مرتبط است', 'P176': 'سازندۀ اصلی(نیازی به آوردن نام سازندگان فرعی نیست)', 'P178': 'شخص یا شرکتی که این آیتم را توسعه داده است', 'P179': '', 'P190': '', 'P194': '', 'P205': 'کشوری که دریاچه در آن قرار دارد یا کشورهایی که در کنار یک دریاچه قرار دارند یا کشورهایی که منبع ورودی آب دریاچه یا مخزن خروجی آب دریاچه در آن ها قرار دارد', 'P206': 'دریاچه ای که مکان مورد نظر در آن واقع شده است', 'P241': '', 'P264': 'برچسب (آرم) نام تجاری که نوار یا سی دی یا ... با آن عنوان تکثیر می شود', 'P272': '', 'P276': 'مکان فعلی یک شیء در حال حرکت', 'P279': 'همهٔ این آیتم\u200cها زیرگروه آن  آیتم هستند؛ این آیتم بخشی از آن آیتم است.', 'P355': 'شرکت\u200cهای زیرمجموعۀ یک شرکت', 'P361': 'این آیتم جزئی است از آیتم ...', 'P364': 'اولین زبانی که فیلم، سی دی ویدئویی، ... مورد نظر (Deprecated for books, written texts, use P407 instead)به آن زبان ایجاد شده است (برای متون مانند کتاب\u200cها از P407 استفاده کنید)', 'P400': 'سکویی که یک اثر توسعه یا منتشر شده/سکوی نسخهٔ خاصی از یک نرم\u200cافزار توسعه\u200cیافته', 'P403': 'در زمینۀ هیدرولوژی', 'P449': 'شبکه\u200cای که اولین بار فیلم تلویزیونی موردنظر از آن پخش شده است', 'P463': 'سازمان یا باشگاهی(شامل گروه\u200cهای قومی یا اجتماعی نمی\u200cشود) که شخص مورد نظر به صورت داوطلبانه در آن عضو شده بود یا عضو آن هست', 'P488': 'میرمسئول یک سازمان یا رئیس یک گروه', 'P495': 'کشوری که این آیتم (اثر خلاقانه، خوراک، سخن، کالا و...) ریشه در آنجا دارد', 'P527': 'جزئی از', 'P551': 'جایی که شخص مورد نظر در آن زندگی می\u200cکرده یا زندگی می\u200cکند', 'P569': 'زمان به\u200cدنیا آمدن شخص مورد نظر', 'P570': 'زمان از دنیا رفتن شخص مورد نظر', 'P571': 'زمان راه\u200eاندازی سازمان یا بنای مورد نظر', 'P576': 'تاریخ فروپاشی یا ادغام سازمان مورد نظر', 'P577': 'زمانی که اثر فرهنگی مورد نظر منتشر شده است', 'P580': 'تاریخ به\u200eوجود\u200e آمدن آیتم مورد نظر یا دارای\u200eاعتبار شدن آن', 'P582': 'تاریخ  از بین رفتن آیتم مورد نظر یا از دست رفتن اعتبار ادعای مورد نظر', 'P585': 'زمان یا تاریخ وقوع یا زمان انتشار مطلب (در بخش منبع)', 'P607': 'جنگ\u200cهایی که شخص موردنظر در آن\u200cها جنیگیده است', 'P674': 'شخصیت\u200cهایی که در آیتم (نمایشنامه، اپرا، کتاب) مورد نظر حضور می\u200cیابند', 'P676': 'سرایندۀ اشعار ترانۀ مورد نظر', 'P706': 'موقعیت یک شی یا زمین\u200cچهره. برای تقسیمات کشوری از P131 استفاده شود', 'P710': 'افراد یا سازمان\u200cهای شرکت کننده در یک رویداد', 'P737': '', 'P740': 'مکانی که سازمان یا گروه مورد نظر در آنجا شکل گرفت', 'P749': '', 'P800': 'اثرهای قابل توجه علمی و هنری', 'P807': 'تأسیس یا شروع شده با جدا شدن از', 'P840': '', 'P937': 'محلی که شاغل در آنجا فعالیتش را انجام داده است', 'P1001': '', 'P1056': 'ماده\u200cای که معدن، مکان صنعتی یا فرآیند تولیدی مورد نظر آن را تولید می\u200cکند', 'P1198': 'بخشی از جمعیت دارای نیروی کار که فاقد کار هستند', 'P1336': 'کشوری که ادعای مالکیت بر منطقه مورد نظر را دارد', 'P1344': 'افراد سهیم در (بر عکس خصوصیت P107)', 'P1365': 'برای سازه جایگزین از P167 و از P155 زمانی که آیتم قبلی وجود دارد و قابل تشخیص است.', 'P1366': 'فرد دومی که جایگزین وی شده\u200cاست', 'P1376': '', 'P1412': 'زبانی که فرد با آن صحبت می\u200cکند یا اثری به آن زبان نوشته است', 'P1441': 'مثلاً به رستم در کتاب شاهنامه اشاره شده است', 'P3373': ''}


def search_by_property(index: str, field: str, value: str, use_keyword: bool = False):
    """
    Search an Elasticsearch index for documents where a field matches a value.

    :param index: Name of the Elasticsearch index (e.g., "triples")
    :param field: Field name to query (e.g., "property")
    :param value: Value to search for (e.g., "PuIo3JcBtCojIGCLAxaj")
    :param use_keyword: Whether to use .keyword for exact match (defaults to False)
    :return: List of matching documents (sources only)
    """
    es = Elasticsearch("http://localhost:9200")

    # Optionally append '.keyword' for exact term query
    query_field = f"{field}.keyword" if use_keyword else field

    # Choose between term and match query
    query_type = "term" if use_keyword else "match"

    query = {
        "query": {
            query_type: {
                query_field: value
            }
        },
        "size":1000
    }

    response = es.search(index=index, body=query)
    result = []
    for hit in response["hits"]["hits"]: 
        triple = hit['_source']       
        if triple['relation'] in property_set:
            result.append(triple)
    return result


def get_triples_score(text, triples, model):
    sentences = []
    for triple in triples:        
        sentences.append(f"{triple['head']} {property_set[triple['relation']]} ({property_description[triple['relation']]}) {triple['tail']}")

    sentences_1 = [text]

    embeddings_1 = model.encode(sentences_1,                             
                                max_length=1000, # If you don't need such a long length, you can set a smaller value to speed up the encoding process.
                                )
    embeddings_2 = model.encode(sentences)
    similarity = embeddings_1 @ embeddings_2.T
    
    for i in range(len(triples)):
        triples[i]['score'] = similarity[0][i]


def filter_by_score(data: List[Dict], node) -> List[Dict]:
    """
    Groups the input list of dictionaries by 'head', then by 'head_id',
    and retains only the subgroup (per head) with the highest cumulative score.

    Args:
        data (List[Dict]): List of dictionaries with 'head', 'head_id', and 'score'.

    Returns:
        List[Dict]: Filtered list with only the highest-scoring subgroup per 'head'.
    """
    result = []
    groups = defaultdict(list)

    # Group by 'head'
    for item in data:
        groups[item[node]].append(item)

    # Within each head group, group by 'head_id' and select highest scoring group
    for _, items in groups.items():
        id_groups = defaultdict(list)
        for item in items:
            id_groups[item[f'{node}_id']].append(item)

        # Select the head_id group with the maximum total score
        max_id = max(
            id_groups.items(),
            key=lambda x: sum(i['score'] for i in x[1])
        )[0]

        result.extend(id_groups[max_id])

    return result

def resolve_entity_qids(data):
    # Count frequencies of (name, qid) pairs for both head and tail
    name_qid_counter = Counter()

    for entry in data:
        name_qid_counter[(entry['head'], entry['head_id'])] += 1
        name_qid_counter[(entry['tail'], entry['tail_id'])] += 1

    # Determine the most frequent qid for each name
    name_to_qid_freq = defaultdict(list)
    for (name, qid), count in name_qid_counter.items():
        name_to_qid_freq[name].append((qid, count))

    name_to_best_qid = {
        name: max(qid_counts, key=lambda x: x[1])[0]
        for name, qid_counts in name_to_qid_freq.items()
    }

    # Filter the data
    filtered_data = []
    for entry in data:
        correct_head_id = name_to_best_qid[entry['head']]
        correct_tail_id = name_to_best_qid[entry['tail']]
        if entry['head_id'] == correct_head_id and entry['tail_id'] == correct_tail_id:
            filtered_data.append(entry)

    return filtered_data


def find_all_phrase_token_spans(text, phrases):    
    encoding = tokenizer(text, return_offsets_mapping=True, return_attention_mask=False, add_special_tokens=False)

    tokens = tokenizer.convert_ids_to_tokens(encoding['input_ids'])
    offsets = encoding['offset_mapping']

    results = {}  # { phrase: [(start_token_idx, end_token_idx), ...] }

    for phrase in phrases:
        phrase_spans = []
        search_start = 0

        while True:
            phrase_start = text.find(phrase, search_start)
            if phrase_start == -1:
                break
            phrase_end = phrase_start + len(phrase)

            # Find token indices for this occurrence
            token_start = token_end = None
            for idx, (start, end) in enumerate(offsets):
                if start >= phrase_start and token_start is None:
                    token_start = idx
                if end <= phrase_end:
                    token_end = idx
                if start > phrase_end:
                    break

            if token_start is not None and token_end is not None and token_start <= token_end:
                phrase_spans.append((token_start, token_end))

            # Move search start past this occurrence
            search_start = phrase_start + 1

        results[phrase] = phrase_spans if phrase_spans else [] #BUG

    return tokens, results


def process_doc(doc):    
    doc_id = doc['_id']
    # print(doc_id)
    triples = search_by_property("triples", "doc_id", doc_id, use_keyword=False)
    # print(f"before filtering by score: {len(triples)}")
    # triples = resolve_entity_qids(triples)
    print(f"before filtering with score triples: {len(triples)}")
    get_triples_score(doc['_source']['abstract'], triples, model)
    triples = filter_by_score(triples, 'head')
    triples = filter_by_score(triples, 'tail')
    print(f"after filtering with score triples: {len(triples)}")
    # print(f"after filtering by score: {len(triples)}")

    entity_dict = {}
    relations = []
    for triple in triples:
        entity_dict[triple['head']] = triple['head_id']
        entity_dict[triple['tail']] = triple['tail_id']
        relations.append((triple['head_id'], triple['tail_id'], triple['relation'], triple['score']))
    
    tokens, results = find_all_phrase_token_spans(doc['_source']['abstract'], entity_dict.keys())

    qid_occurence = {}
    for alias in results.keys():
        qid = entity_dict[alias]
        if qid in qid_occurence.keys():
            try:
                qid_occurence[qid] += results[alias]
            except:
                print(results)
                return
        else:
            qid_occurence[qid] = results[alias]
    return {
        'title': doc['_source']['title'],
        'tokens':tokens,
        'qid_pos': qid_occurence,
        'relations': relations
    }



def process_batch(batch):
    for doc in batch:
        print(doc['_id'])
        abstract = doc["_source"]['abstract']
        if len(abstract.split()) > 128:
            result = process_doc(doc)            
            print(result)
        print("="*30)

        
def main():    
            
        

    # Initial search to create a scroll context
    response = es.search(
        index="wiki_abstracts",
        scroll="2m",
        size=1,
        body={
            "query": {
                "match_all": {}
            }
        }
    )

    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]

    # Initial batch processing
    process_batch(hits)

    batch_count = 0
    while hits:
        if batch_count == 10:
            break

        response = es.scroll(
            scroll_id=scroll_id,
            scroll="2m"
        )
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]

        process_batch(hits)

        batch_count += 1

    # Always clear scroll in production
    es.clear_scroll(scroll_id=scroll_id)


if __name__ == "__main__":
    main()
