"""Build the HTML submission from the raw measurements and generated summaries."""
from html import escape

from analyze import read
from cluster import RESULTS, ROOT

DOC = ROOT / "report.html"


def table(headers, rows):
    heading = "".join(f"<th scope='col'>{escape(str(h))}</th>" for h in headers)
    contents = "".join("<tr>" + "".join(f"<td>{escape(str(v))}</td>" for v in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{heading}</tr></thead><tbody>{contents}</tbody></table>"


def fmt(value, digits=3):
    return "—" if value is None else f"{value:.{digits}f}"


def plot(name, caption):
    url = f"results/figures/{name}"
    if not (ROOT/url).exists():
        return ""
    return f'<figure><img src="{url}" alt="{escape(caption)}"><figcaption>{escape(caption)}</figcaption></figure>'


def link(path, label):
    return f'<a href="{escape(path, quote=True)}">{escape(label)}</a>'


def warmup(roll):
    lead = """<h2>Опыт A. Прогрев, выпуск и откат</h2>
<p><b>Гипотеза.</b> Если готовность совпадает с открытием HTTP-порта, Service направляет запросы в B во время прогрева. Новая версия прогревается 20 с. Перед заменой A запускаем поток на 12 частей, затем повторяем обновление под тем же фоновым трафиком с исправленными проверками.</p>
<p><b>Исправление.</b> Startup подтверждает чтение артефакта, совпадение версии и SHA-256 и завершение прогрева. Readiness допускает рабочий трафик после startup. Liveness проверяет доступность HTTP-процесса. Kubernetes обновляет EndpointSlice при завершении Pod. В штатном обновлении <code>maxUnavailable=0</code>, <code>maxSurge=1</code>. <code>preStop</code> даёт 4 с на обновление маршрутизации, Uvicorn ждёт активные соединения до 25 с, общий grace period 35 с.</p>"""
    if not all(key in roll for key in ("broken", "fixed", "rollback", "forced")):
        return lead + "<p>Полный набор измерений A ещё не собран.</p>"
    rows = []
    for mode in ("broken","fixed","rollback"):
        s = roll[mode]["stats"]
        rows.append((mode,s["planned"],s["successful"],s["http_5xx"],s["rejected"],s["aborted"],s["timeout"],
                     ", ".join(f"{k}: {v}" for k,v in roll[mode]["versions"].items())))
    lead += table(["Прогон","Запланировано","OK","5xx","429","Оборваны","Тайм-ауты","Версия в ответе"],rows)
    streams = roll["fixed"]["extra"]
    when = next(r["unix"] for r in roll["fixed"]["events"] if r["event"] == "upgrade_A_to_B")
    late = [(r["request_id"], r["pod"], fmt(r["sent_unix"]-when,2), fmt(r["end_unix"]-when,2), r["chunks"],
             r["version"], r["outcome"]) for r in streams]
    lead += plot("01-rollout.png","Начало обновления — 0 с. Маркеры 503 показывают запросы во время прогрева B.")
    lead += "<h3>Потоки через границу обновления</h3>"
    lead += table(["Запрос","Pod","Отправлен, с","Завершён, с","Части","Версия","Результат"],late)
    rollback = roll["rollback"]["extra"]
    forced = roll["forced"]["requests"]
    bad = rollback[0]
    interrupted = forced["interrupted"]
    at = roll["forced"]["events"]
    killed = next(r["unix"] for r in at if r["event"]=="force_delete")
    recovered = next(r["unix"] for r in at if r["event"]=="replacement_ready")
    collateral = {}
    for r in forced["collateral"]:
        collateral[r["outcome"]] = collateral.get(r["outcome"],0)+1
    lead += (f"<p><b>Откат.</b> B запустилась с артефактом A и отчиталась об ошибке проверки. "
             f"Прямой запрос к B вернул HTTP {bad['status']}. В рабочем EndpointSlice B оставалась неготовой. "
             f"После <code>helm rollback</code> все {len(rollback)-1} контрольных запросов обслужены A. "
             f"Полный хеш артефакта A в результатах совпадает с файлом в чарте.</p>"
             f"<p><b>Принудительная потеря.</b> Поток уже имел HTTP {interrupted['status']} и успел передать "
             f"{interrupted['chunks']} частей из 24, закрывающая запись <code>done</code> отсутствует. "
             f"Попытка при переполнении отдельно получила HTTP {forced['rejection']['status']}. "
             f"Исходы ожидающих запросов: {escape(', '.join(f'{k}: {v}' for k,v in sorted(collateral.items())))}. "
             f"Проверка через Service после удаления достигла уцелевшей реплики. Новый Pod стал Ready через "
             f"{recovered-killed:.1f} с от команды удаления, повторно загрузил версию A. Два диалога продолжились "
             f"с прежними разными контрольными строками после передачи истории в запросе.</p>"
             "<p><b>Граница результата.</b> Grace period покрывает измеренную нагрузку и выбранные пределы потока. "
             "При принудительной остановке ответ может оборваться после заголовков 200. Клиент проверяет конец "
             "потока. В опыте повторы отключены, каждая попытка учитывается один раз.</p>")
    files = [("broken/timeline.jsonl","Ready/EndpointSlice до исправления"), ("fixed/timeline.jsonl","после исправления"),
             ("rollback/bad-pod.jsonl","ошибка загрузки B"), ("rollback/timeline.jsonl","линия отката"),
             ("forced/requests.json","результаты принудительной потери")]
    lead += "<p>" + " · ".join(link("results/rollout/"+path, name) for path,name in files) + ".</p>"
    return lead


def balance(runs):
    html = """<h2>Опыт B. ClientIP и горячая реплика</h2>
<p><b>Гипотеза.</b> Когда 80% запросов идёт от одного IP, ClientIP отправляет их в один Pod и удлиняет очередь. Обычный Service позволяет распределять новые соединения между двумя репликами. Оба Service выбирают один Deployment. Один запуск клиента открывает отдельное TCP/HTTP-соединение на попытку и отправляет <code>Connection: close</code>. Один узел kind, сеть kindnet, маршрутизация по конфигурации kube-proxy. IP сверены по Downward API у клиента и по адресу на сервере.</p>
<p><b>Метод.</b> Четыре клиента. Равномерный вариант делит попытки поровну. В сценарии перекоса Pod клиента 0 получает четыре из каждых пяти попыток, оставшиеся распределены по остальным трём клиентам. У обеих политик одинаковые 16 префиксов, длины входов, четыре части ответа, пауза 24 мс на часть и расписание 7,5 запроса/с на протяжении 120 с. Перед каждым повтором заменяются обе реплики, проверяется готовность. Кеш в начале пуст. Его ёмкость 16 префиксов, CPU-работа PBKDF2 — 50 000 итераций на попадание, 180 000 на промах и 90 000 на часть. На Pod один активный запрос и до восьми ожидающих.</p>"""
    def pair(value):
        return " / ".join(fmt(value[p]) for p in ("p50", "p95"))

    rows = []
    for run in runs:
        s = run["stats"]
        rows.append((run["schedule"]["run"], s["planned"], s["sent"], s["successful"], s["rejected"],
                     s["aborted"], s["timeout"], s["http_5xx"], pair(s["ttfc"]), pair(s["queue"]),
                     pair(s["total"]), pair(s["inter_chunk"]), fmt((s["send_lag"]["p95"] or 0)*1e3,1),
                     fmt(s["cache_hits"]/s["successful"]*100 if s["successful"] else None,1)))
    if rows:
        html += "<p>Для задержек указаны p50 / p95, n равен числу OK. Для интервалов между частями n = 3 × OK.</p>"
        html += table(["Прогон","План","Отпр.","OK","429","Обрыв","Тайм-аут","5xx","TTFC, с",
                       "Очередь, с","Полный ответ, с","Между частями, с","Опоздание p95, мс","Кеш hit, %"],rows)
    html += plot("02-replicas.png","Каждый столбец — Pod. Доля нагрузки учитывает и успешные потоки, и HTTP 429.")
    html += plot("03-policies.png","В каждой группе отдельные точки трёх повторов. Перцентили рассчитаны по завершённым потокам.")
    if len(runs) == 12:
        a = [r for r in runs if r["schedule"]["traffic"]=="skew" and r["schedule"]["policy"]=="sticky"]
        b = [r for r in runs if r["schedule"]["traffic"]=="skew" and r["schedule"]["policy"]=="plain"]
        hot = max(a,key=lambda r:max(p["planned"] for p in r["pod"].values()))
        hottest = max(hot["pod"].values(), key=lambda p:p["planned"])
        coldest = min(hot["pod"].values(), key=lambda p:p["planned"])
        split = [(r["schedule"]["run"], sorted((p["planned"] for p in r["pod"].values()),reverse=True))
                 for r in runs if r["schedule"]["policy"]=="sticky" and r["schedule"]["traffic"]=="uniform"]
        balanced = min(split, key=lambda x:max(x[1]))
        html += (f"<p><b>Причина.</b> В {hot['schedule']['run']} самая загруженная реплика получила "
                 f"{hottest['planned']} запросов из {hot['stats']['planned']}. На ней p95 очереди "
                 f"{fmt(hottest['queue']['p95'])} с и {hottest['rejected']} отказов. На второй "
                 f"{coldest['planned']} запросов и p95 очереди {fmt(coldest['queue']['p95'])} с. "
                 f"При равномерном притоке наиболее близкое к равномерному распределение ClientIP было "
                 f"в {balanced[0]} ({' / '.join(str(x) for x in balanced[1])}). "
                 "В других повторах несколько клиентских IP могут привязаться к одному Pod. "
                 "Это видно в матрице «IP → Pod» в анализе. Привязка стабилизирует выбор для IP, "
                 "а начальный выбор Pod для разных IP остаётся независимым.</p>"
                 f"<p><b>Изменение.</b> Для этой нагрузки выбран Service без привязки. При перекосе "
                 f"общий p95 TTFC с ClientIP по трём прогонам: {', '.join(fmt(r['stats']['ttfc']['p95']) for r in a)} с. "
                 f"После изменения: {', '.join(fmt(r['stats']['ttfc']['p95']) for r in b)} с. "
                 f"Отказы 429 по трём повторам: {', '.join(str(r['stats']['rejected']) for r in a)} → "
                 f"{', '.join(str(r['stats']['rejected']) for r in b)}. "
                 "Размывание локальности допускаем, поскольку выигрыш на очереди измерен. Реальный inference engine "
                 "потребует отдельно измерить стоимость prefill, hit rate, TTFT и экономию времени GPU.</p>")
    html += "<p>" + " · ".join((link("results/balance/network.json","Сеть и конфигурации Service"),
        link("results/balance/","Журналы 12 прогонов"),link("results/analysis.json","Матрицы источников и реплик"))) + ".</p>"
    return html


def metric_comparison(runs):
    rows = []
    for run in runs:
        prom = run.get("promql", {})
        for name, server, client in (("TTFC","hw11_ttfc_seconds_bucket","ttfc"),
                                      ("Весь поток","hw11_duration_seconds_bucket","total")):
            if f"{server}:p95" not in prom:
                continue
            buckets = prom[f"{server}:buckets"]["result"]
            plus_inf = next((float(r["value"][1]) for r in buckets if r["metric"].get("le")=="+Inf"), None)
            val = lambda p: float(prom[f"{server}:{p}"]["result"][0]["value"][1]) if prom[f"{server}:{p}"]["result"] else None
            stats = run["stats"][client]
            rows.append((run["schedule"]["run"],name,stats["n"],fmt(plus_inf,0),fmt(stats["p50"]),fmt(val("p50")),
                         fmt(stats["p95"]),fmt(val("p95"))))
    intro = """<h3>Сверка гистограмм и клиентского журнала</h3>
<p>Клиент считает TTFC от отправки по монотонным часам до первой содержательной NDJSON-части, длительность ответа до закрывающей записи, inter-chunk latency как интервалы между соседними частями. Для каждой статистики p50/p95 берём только завершившиеся потоки и показываем их число. Вычисление CPU, сетевой переход и границы bucket объясняют разницу с серверной оценкой.</p>
<pre><code>histogram_quantile(0.95,
  sum by (le, pod) (rate(hw11_ttfc_seconds_bucket[30s])))</code></pre>
<p>Дашборд использует rate для скользящего окна. Для сверки с конкретным опытом из суммарных bucket-счётчиков вычитается снимок до измерения из снимка после обработки хвоста и следующего scrape. Запрос ограничен той же парой Pod, включены те же успешные потоки. Полные PromQL и результаты сохранены в <code>promql.json</code> соответствующих прогонов.</p>"""
    if rows:
        intro += table(["Прогон","Метрика","n клиент","n Prometheus","p50 клиент, с","p50 Prometheus, с",
                        "p95 клиент, с","p95 Prometheus, с"],rows)
    return intro


def autoscale(hpa):
    html = """<h2>Опыт C. HPA и алерты</h2>
<p><b>Гипотеза.</b> Когда CPU-нагрузка превышает 50% от запросов CPU, HPA добавит третью реплику. При существующей привязке ClientIP ранее назначенные клиенты могут остаться на старых Pod, несмотря на Ready у новой реплики. HPA <code>autoscaling/v2</code> использует metrics-server и CPU request 100m у api. Диапазон 2–3 реплики, окно уменьшения 60 с. Прогрев 12 с. Стоимость увеличена до 160 000 итераций на cache hit, 380 000 на miss и 220 000 на часть. Высокая фаза — 13 запросов/с × 105 с, пять частей и 16 мс на часть. Низкая — 0,9 запроса/с × 105 с, одна часть и 16 мс.</p>
<p>Два правила удерживают условие 12 с, окно расчёта 30 с. Для TTFC p95 &gt; 0,6 с требуется минимум 15 успешных потоков. Для отказов 429 &gt; 10% требуется минимум 20 исходов. Prometheus проверяет правила каждые 3 с.</p>"""
    if not hpa:
        return html + "<p>Полный цикл HPA ещё не сохранён.</p>"
    base = hpa["high_start"]
    rows = ((name,fmt(hpa[key]-base if hpa[key] else None,1)) for name,key in
            [("Переход desired на 3","scale_up"),("Третья реплика Ready","third_ready"),
             ("Начало низкой нагрузки","low_start"),("Возврат к двум Ready","scale_down")])
    html += table(["Событие","Время от начала высокой фазы, с"],rows)
    if hpa["scale_down"]:
        html += f"<p>От начала низкой фазы до возврата к двум Ready прошло {hpa['scale_down']-hpa['low_start']:.1f} с. Окно стабилизации отсчитывается контроллером по истории рекомендаций, поэтому фактический интервал включает сбор CPU и циклы контроллера.</p>"
    if hpa["scale_down"] and hpa["scale_down"] > hpa["low_end"]:
        html += (f"<p>От последнего завершившегося запроса низкой фазы до двух Ready прошло "
                 f"{hpa['scale_down']-hpa['low_end']:.1f} с. Пока низкий трафик продолжался, "
                 "средняя CPU держалась около 33–35% request. При трёх репликах переход к двум "
                 "требует значения около 33% или ниже и устойчивой рекомендации на всём окне.</p>")
    if hpa["third"]:
        third = hpa["third"]
        pod = third["pod"]
        during = hpa["after_three"]
        n = during.get(pod,{}).get("planned",0)
        total = sum(p["planned"] for p in during.values())
        html += (f"<p>Третий Pod <code>{escape(pod)}</code> создан {escape(third['created'])}, "
                 f"Ready с {escape(third['ready_transition'])} UTC по API. После его готовности "
                 f"он получил {n} из {total} запросов высокой фазы по ClientIP. "
                 "Среднее CPU может вырасти и от одного горячего Pod. Если среднее ниже порога, HPA по CPU "
                 "не увидит очередь. Под нагрузкой полезно смотреть waiting и долю отказов по каждому Pod.</p>")
    html += plot("04-hpa.png","Вертикальная линия — начало низкой фазы. CPU показана относительно requests.")
    html += table(["Фаза","План","OK","429","Оборваны","Тайм-ауты","p95 TTFC, с"],
        [(name,*[hpa[name][k] for k in ("planned","successful","rejected","aborted","timeout")],
          fmt(hpa[name]["ttfc"]["p95"])) for name in ("high","low")])
    html += table(["Правило","Состояния (время от высокой нагрузки, с)"],
        [(name," → ".join(f"{state} ({when:.1f})" for when,state in transitions)) for name,transitions in hpa["alerts"].items()])
    html += ("<p>" + " · ".join((link("results/autoscale/timeline.jsonl","Сырые CPU, условия HPA, Ready и алерты"),
             link("results/autoscale/configuration.json","Конфигурация HPA"),link("results/grafana-export.json","Экспорт Grafana"))) + ".</p>")
    return html


def main():
    data = read(RESULTS / "analysis.json")
    docker = read(RESULTS / "docker-limits.json")
    k8s = read(RESULTS / "kubernetes.json")
    available = (RESULTS / "free-space-before.txt").read_text().splitlines()[1].split()[3]
    size_path = RESULTS / "node-usage-final.txt"
    size = size_path.read_text().strip() if size_path.exists() else (RESULTS / "node-usage-initial.txt").read_text().strip()
    runtime = table(["Параметр","Значение"],[
        ("Kubernetes / kind / Helm", f"{k8s['serverVersion']['gitVersion']} / v0.30.0 / v3.18.6"),
        ("Контейнеры", f"{docker['OperatingSystem']} {docker['ServerVersion']}, {docker['Architecture']}"),
        ("Лимиты Docker", f"{docker['NCPU']} CPU, {docker['MemTotal']/2**30:.2f} GiB"),
        ("Диск до запуска", available),
        ("API (запрос / лимит на Pod)", "100m / 800m CPU, 64Mi / 128Mi RAM"),
        ("Реплики", "2, HPA 2–3 в опыте C"),
        ("Хранилище метрик", "24 ч, 256 MB retention, том Prometheus 320 MiB, Grafana 160 MiB"),
        ("Режим", "simulated, Python 3.12.11, CPU")])
    check_path = RESULTS / "checks.txt"
    check_html = f"<pre>{escape(check_path.read_text())}</pre>" if check_path.exists() else "<p>Проверки ещё не завершены.</p>"
    pages = [
        """<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>ДЗ 11 · Kubernetes под нагрузкой</title>
<style>
:root{font-family:system-ui,sans-serif;line-height:1.55;color:#152238;background:#f4f7fc}
body{margin:0}main{max-width:1140px;margin:auto;padding:36px 30px 72px;background:#fff}
h1{line-height:1.1;font-size:2.5rem;margin:0.2em 0}.lead{font-size:1.12rem;max-width:82ch}
h2{margin-top:2.1em;border-bottom:1px solid #dbe3ef;padding-bottom:.25em}h3{margin-top:1.55em}
p{max-width:100ch}a{color:#1855a9}table{border-collapse:collapse;font-size:.87rem;width:100%;margin:1.1em 0}
th,td{text-align:left;padding:.52em .5em;border-bottom:1px solid #dbe3ef;vertical-align:top;overflow-wrap:anywhere}
th{background:#edf3fc}pre{overflow:auto;background:#eef3f9;padding:1em;border-radius:6px}
code{overflow-wrap:anywhere}figure{margin:1.3em 0}img{max-width:100%;height:auto;border:1px solid #dbe3ef}
figcaption{color:#435674;font-size:.91rem;margin-top:.45em}.meta{color:#455d7c}
@media(max-width:750px){main{padding:20px 14px}table{display:block;overflow:auto;white-space:nowrap}}
@media print{main{padding:0}figure,tr{break-inside:avoid}}
</style></head><body><main><p class="meta">МГТУ · MLOPS-26 · ДЗ 11</p>
<h1>Сервис работает, пользователи ждут</h1>
<p class="lead">API из ДЗ 8–10 развёрнут в Kubernetes с двумя репликами. Проверены готовность при прогреве, потоки при обновлении, распределение очередей и рост числа реплик по CPU.</p>
<h2>Стенд</h2>
<p>Сохранён контракт <code>/health</code>, <code>/ready</code>, <code>/generate</code> и проверка загруженной версии из предыдущего API. В этой работе <code>/generate</code> отдаёт части NDJSON, <code>max_new_tokens</code> задаёт предел частей имитатора. Ответы и структурированные логи содержат <code>request_id</code>, Pod, загруженную версию и SHA-256, а история передаётся в теле каждого запроса. Логи содержат session ID, метки метрик ограничены Pod и исходом.</p>
<p>Чарт монтирует маленькие артефакты A и B как immutable ConfigMap с именем на основе SHA-256. Процесс пересчитывает хеш файла и сверяет фактическую версию с ожидаемой. Helm фиксирует источники артефактов каждого релиза. На новом Pod загружается тот же файл из Kubernetes API.</p>""",
        runtime,
        f"<p>Использование диска внутри узла после развёртывания, по каталогам:</p><pre>{escape(size)}</pre>",
        """<p>Версии и дайджесты в
<a href="results/images.txt">инвентаризации образов</a>. Как поднять и удалить стенд, описано в
<a href="README.md">README</a>. Для сбора метрик Prometheus открывает /metrics каждого Pod через kubernetes_sd.
Grafana показывает TTFC, полный ответ, running/waiting, успешные запросы, отказы, CPU, RSS и Ready-реплики.</p>""",
        warmup(data["rollout"]),
        balance(data["balance"]),
        metric_comparison(data["balance"]),
        autoscale(data["autoscale"]),
        """<h2>Пять проверок</h2>
<p>Команда <code>make check-live</code> воспроизводит нагрузку и HTTP-проверки в Kubernetes. <code>make check</code> проверяет уже сохранённые результаты.</p>""",
        check_html,
        """<p><a href="results/requests.csv">Единый CSV по всем попыткам</a>. В сырых JSONL сохраняются время по монотонным часам, запланированное время, фактическая отправка, первая часть, завершение, HTTP-статус, исход, Pod, версия, число частей и интервалы. Время UTC служит для сопоставления с Kubernetes и Prometheus. Клиент использует монотонные часы для длительностей. Повторы в опытах выключены.</p>
<h2>Ревью PR. Привязка диалога</h2>
<p>Я бы отправил запрос в B. Она пересчитает префикс за 0,8 с, а на A одна очередь займёт 3 с. Для каждого нового запроса я бы сравнивал ожидаемое время в очереди и стоимость пересчёта у готовых реплик с нужной версией модели. Перед внедрением я бы замерил эти времена для разных размеров контекста и сравнил p95 TTFT и долю отказов под нагрузкой. Если у подходящих реплик заполнены очереди, верну 429 до начала потока.</p>
<p>Историю диалога передаёт клиент. Если A потеряна, клиент отправляет ту же историю в B со следующим запросом. При плановом отключении Pod даю уже начатому потоку закончиться в пределах grace period. При аварии клиент помечает ответ как неполный. Повтор идёт отдельным запросом с новым ID.</p>
<p>В кейсе из лекции Google Cloud увеличил вес очереди при выборе реплики в GKE Inference Gateway. Это позволяло направить запрос мимо перегруженной реплики, даже если на ней уже был нужный префикс. <a href="https://cloud.google.com/blog/products/containers-kubernetes/how-gke-inference-gateway-improved-latency-for-vertex-ai">Первоисточник Google Cloud</a>.</p>
</main></body></html>"""
    ]
    DOC.write_text("\n".join(pages) + "\n")


if __name__ == "__main__":
    main()
