from elasticsearch import Elasticsearch
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from transformers.cache_utils import DynamicCache
import gc

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")
persian_property_set = {'P6': 'رئیسجمهور یا شهردار', 'P17': 'کشور', 'P19': 'زادگاه', 'P20': 'محل مرگ', 'P22': 'پدرش', 'P25': 'مادرش', 'P26': 'همسر', 'P27': 'تبعۀ', 'P30': 'قاره', 'P31': 'نمونهای از', 'P35': 'حاکم', 'P36': 'پایتخت', 'P37': 'زبان رسمی', 'P39': 'منصب', 'P40': 'فرزند', 'P50': 'نویسنده', 'P54': 'عضو تیم ورزشی', 'P57': 'کارگردان', 'P58': 'فیلمنامهنویس', 'P69': 'دانشآموختهٔ', 'P86': 'آهنگساز', 'P102': 'عضو حزب', 'P108': 'کار میکند/میکرد در', 'P112': 'بنیانگذار', 'P118': 'لیگ', 'P123': 'ناشر', 'P127': 'دارنده', 'P131': 'موقعیت در تقسیمات کشوری', 'P136': 'سبک', 'P137': 'کارفرما', 'P140': 'دین', 'P150': 'زیربخش', 'P155': 'پس از', 'P156': 'پیش از', 'P159': 'محل ستاد', 'P161': 'با هنرمندی', 'P162': 'تهیهکننده', 'P166': 'جوایز', 'P170': 'پدیدآورنده', 'P171': 'آرایهٔ والد', 'P172': 'قوم', 'P175': 'اجرا کننده', 'P176': 'سازنده', 'P178': 'توسعهدهنده', 'P179': 'دنباله', 'P190': 'شهرهای خواهرخوانده', 'P194': 'مجلس', 'P205': 'کشورهای ساحلی دریاچه', 'P206': 'آب سطحی پیرامون', 'P241': 'شاخهٔ نظامی', 'P264': 'برچسب ضبط', 'P272': 'شرکتهای رسانه', 'P276': 'مکان کنونی', 'P279': 'زیرردۀ', 'P355': 'شرکتهای زیرمجموعه', 'P361': 'جزئی از', 'P364': 'زبان اصلی', 'P400': 'سکو', 'P403': 'میریزد به', 'P449': 'پخش شده از شبکه یا کانالِ', 'P463': 'از اعضای', 'P488': 'مدیر مسئول', 'P495': 'زادگاه فرهنگی', 'P527': 'دربرگیرنده', 'P551': 'جای زندگی', 'P569': 'زادروز', 'P570': 'درگذشت', 'P571': 'تاریخ ساخت\u202f/\u2009تأسیس', 'P576': 'تاریخ فروپاشی', 'P577': 'تاریخ انتشار', 'P580': 'زمان آغاز', 'P582': 'زمان پایان', 'P585': 'زمان رویداد', 'P607': 'جنگها', 'P674': 'شخصیتها', 'P676': 'سرایندۀ اشعار', 'P706': 'موقعیت', 'P710': 'با حضورِ', 'P737': 'متأثر از', 'P740': 'مکان شکلگیری', 'P749': 'نهاد ارشد', 'P800': 'اثر (ها)', 'P807': 'جداشده از', 'P840': 'مکان وقوع داستان', 'P937': 'محل انجام کار', 'P1001': 'متعلق به حوزهٔ قضایی', 'P1056': 'تولیدکنندهٔ', 'P1198': 'نرخ بیکاری', 'P1336': 'ادعا کننده مالکیت', 'P1344': 'شرکت داشته است در', 'P1365': 'به جای', 'P1366': 'جایگزینشده توسط', 'P1376': 'پایتخت است برای', 'P1412': 'زبانهای شخص', 'P1441': 'اشاره شده در کتابِ', 'P3373': 'همنیا'}

def filter_persian_triples(triples):
  new_triples = []
  for triple in triples:
    if triple['relation'] in persian_property_set.keys():
      new_triples.append(triple)
  return new_triples


def get_relations(doc_id, size):
    # Define the index and query
    index_name = "triples"
    query = {
        "query": {
            "term": {
                "doc_id": doc_id
            }
        },
        'size': size
    }

    # Execute the search
    response = es.search(index=index_name, body=query)

    # Print the results
    triples = []
    for hit in response['hits']['hits']:
        doc = hit['_source']
        triples.append(doc)
    triples = filter_persian_triples(triples)
    return triples


def get_article(doc_id):
    index_name = "wiki_abstracts"

    response = es.get(index=index_name, id=doc_id)

    return response['_source']


# ---------- Utilities for caches ----------

def build_kv_caches_batched(model, tokenizer, contexts):
    """Run the model on a batch of contexts and return a list of DynamicCache,
       one per input in the batch (trimmed to each sample's true length)."""
    device = next(model.parameters()).device

    enc = tokenizer(contexts, return_tensors="pt", padding=True, truncation=True)
    input_ids = enc["input_ids"].to(device)
    attention_mask = enc["attention_mask"].to(device)

    # make sure pad_token exists
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token_id = tokenizer.eos_token_id

    batched_cache = DynamicCache()
    model.eval()
    with torch.no_grad():
        _ = model(input_ids=input_ids, attention_mask=attention_mask,
                  past_key_values=batched_cache, use_cache=True)

    batch_size = input_ids.size(0)
    true_lengths = attention_mask.sum(dim=1).tolist()
    split_caches = [DynamicCache() for _ in range(batch_size)]

    for layer_idx in range(len(batched_cache)):
        key_layer, value_layer = batched_cache[layer_idx]  # (B, H, Smax, D)
        for i in range(batch_size):
            L = int(true_lengths[i])
            k_slice = key_layer[i : i + 1, :, :L, :].clone()
            v_slice = value_layer[i : i + 1, :, :L, :].clone()
            split_caches[i].update(k_slice, v_slice, layer_idx)

    return split_caches


def repeat_kv_cache(cache: DynamicCache, repeat_count: int) -> DynamicCache:
    """Repeat each batch entry in cache along batch dimension repeat_count times."""
    repeated = DynamicCache()
    for layer_idx in range(len(cache)):
        k, v = cache[layer_idx]
        k_rep = k.repeat_interleave(repeat_count, dim=0).clone()
        v_rep = v.repeat_interleave(repeat_count, dim=0).clone()
        repeated.update(k_rep, v_rep, layer_idx)
    return repeated


# ---------- Generation helpers ----------

def generate_with_cache(model, input_ids, past_cache: DynamicCache = None, max_new_tokens: int = 20):
    """
    Greedy generation:
      - Appends the given input_ids into past_cache incrementally (one token at a time).
      - Then generates max_new_tokens tokens autoregressively.
    Returns (generated_ids, updated_cache).
    """
    model.eval()
    device = next(model.parameters()).device
    generated = input_ids.to(device)

    with torch.no_grad():
        # append the prefix tokens (e.g., question) incrementally
        for t in range(generated.size(1)):
            token_t = generated[:, t : t + 1]
            outputs = model(input_ids=token_t, past_key_values=past_cache, use_cache=True)
            past_cache = outputs.past_key_values

        # now autoregressive generation
        for _ in range(max_new_tokens):
            last_token = generated[:, -1:]
            outputs = model(input_ids=last_token, past_key_values=past_cache, use_cache=True)
            logits = outputs.logits[:, -1, :]
            next_token = torch.argmax(logits, dim=-1, keepdim=True)
            past_cache = outputs.past_key_values
            generated = torch.cat([generated, next_token], dim=1)

    return generated, past_cache


# ---------- Top-level function ----------

def generate_batch_from_contexts_and_questions(model, tokenizer, contexts, questions_per_context, max_new_tokens=50):
    """
    contexts: List[str] of length C
    questions_per_context: List[List[str]] with same length C

    Returns: list[str] answers aligned with flattened (context, question) order
    """
    device = next(model.parameters()).device

    # 1) Build per-context caches
    kv_caches = build_kv_caches_batched(model, tokenizer, contexts)

    # 2) Repeat caches per question
    repeated_caches = []
    flat_questions = []
    per_context_counts = []
    for cache, questions in zip(kv_caches, questions_per_context):
        n = len(questions)
        per_context_counts.append(n)
        if n == 0:
            continue
        repeated_cache = repeat_kv_cache(cache, n)
        repeated_caches.append(repeated_cache)
        flat_questions.extend(questions)

    if len(flat_questions) == 0:
        return []

    # 3) Concatenate repeated caches into final_cache
    final_cache = DynamicCache()
    num_layers = len(repeated_caches[0])
    for layer_idx in range(num_layers):
        keys_cat = torch.cat([cache[layer_idx][0] for cache in repeated_caches], dim=0)
        vals_cat = torch.cat([cache[layer_idx][1] for cache in repeated_caches], dim=0)
        final_cache.update(keys_cat, vals_cat, layer_idx)

    # 4) Tokenize questions
    q_enc = tokenizer(["\nQuestion: " + q for q in flat_questions],
                      return_tensors="pt", padding=True, truncation=True)
    q_input_ids = q_enc["input_ids"].to(device)
    q_attention_mask = q_enc["attention_mask"].to(device)

    # 5) Generate
    generated_ids, _ = generate_with_cache(model,
                                           input_ids=q_input_ids,
                                           past_cache=final_cache,
                                           max_new_tokens=max_new_tokens)

    # 6) Extract only generated answers
    q_lens = q_attention_mask.sum(dim=1).tolist()
    answers = []
    for i, qlen in enumerate(q_lens):
        new_tokens = generated_ids[i, qlen:].tolist()
        answers.append(tokenizer.decode(new_tokens, skip_special_tokens=True).strip())

    return answers


def free_inference_memory(vars_to_del: list = None, clear_cuda: bool = True):
    """
    Utility to free intermediate memory after inference.
    
    Args:
        vars_to_del (list): List of variable names (as strings) to delete from globals().
        clear_cuda (bool): If True, also clear CUDA memory cache.
    """
    if vars_to_del:
        g = globals()
        for name in vars_to_del:
            if name in g:
                try:
                    del g[name]
                except Exception:
                    pass
    
    # Force garbage collection
    gc.collect()
    
    # Clear CUDA cache if enabled
    if clear_cuda and torch.cuda.is_available():
        torch.cuda.empty_cache()
        torch.cuda.ipc_collect()
