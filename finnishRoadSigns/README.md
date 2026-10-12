# Teoriakoe FI – Mağazada yayınlama kontrol listesi

Sürüm **1.7.0 (build 10)**, ilk mağaza sürümü. Bu klasör Google Play ve App Store için tüm metinleri ve politika sayfalarını içerir. Dosya düzeni fastlane ile uyumludur (`fastlane supply` / `fastlane deliver`); istemezseniz metinleri konsollara kopyala-yapıştır yapabilirsiniz.

| Ne | Nerede |
|---|---|
| Google Play metinleri (en-US, fi-FI, sv-SE) | `store/android/metadata/<dil>/title.txt`, `short_description.txt`, `full_description.txt`, `changelogs/10.txt` |
| App Store metinleri (en-US, fi, sv) | `store/ios/metadata/<dil>/name.txt`, `subtitle.txt`, `keywords.txt`, `description.txt`, `promotional_text.txt`, `release_notes.txt`, `support_url.txt`, `privacy_url.txt`, `marketing_url.txt` |
| App Store ortak alanlar | `store/ios/metadata/copyright.txt`, `primary_category.txt`, `secondary_category.txt`, `review_information/*` |
| Gizlilik politikası (EN/FI/SV) | `store/privacy-policy.html` |
| Destek / SSS sayfası (EN/FI/SV) | `store/support.html` |
| Ekran görüntüleri ve öne çıkan grafik | `store/android/metadata/<dil>/images/`, `store/ios/screenshots/` |
| Karakter sınırı testi | `test/store_metadata_test.dart` |

Metinlerde değişiklik yaptıktan sonra her zaman şunu çalıştırın (sınırlar, anahtar kelime biçimi, feragat ve Autoliitto atfı kontrol edilir):

```sh
flutter test test/store_metadata_test.dart
```

---

## 1. Yer tutucuları doldurun

- [ ] `SUPPORT_EMAIL` → `privacy-policy.html`, `support.html`, `ios/metadata/review_information/email_address.txt`
- [ ] `SUPPORT_URL` → `ios/metadata/{en-US,fi,sv}/support_url.txt` (barındırılan `support.html` adresi)
- [ ] `PRIVACY_POLICY_URL` → `ios/metadata/{en-US,fi,sv}/privacy_url.txt` (barındırılan `privacy-policy.html` adresi)
- [ ] `MARKETING_URL` (isteğe bağlı) → `marketing_url.txt` dosyaları şu an **boş**; web siteniz yoksa boş kalsın
- [ ] `DEVELOPER_NAME` → `ios/metadata/copyright.txt` (`2026 Adınız`), iki HTML sayfası
- [ ] `FIRST_NAME`, `LAST_NAME`, `PHONE_NUMBER` → `ios/metadata/review_information/` (telefon uluslararası biçimde, ör. `+358 40 123 4567`; yalnızca Apple inceleme ekibi görür)
- [ ] Kalan yer tutucu var mı kontrol edin:

```sh
grep -rn "SUPPORT_EMAIL\|SUPPORT_URL\|PRIVACY_POLICY_URL\|DEVELOPER_NAME\|FIRST_NAME\|LAST_NAME\|PHONE_NUMBER" store/
```

## 2. Gizlilik ve destek sayfalarını herkese açık yayınlayın

Her iki mağaza da **herkese açık, giriş gerektirmeyen** bir gizlilik politikası URL'si ister. Örnek: GitHub Pages.

- [ ] GitHub'da herkese açık yeni bir depo açın (ör. `teoriakoe-fi`).
- [ ] `privacy-policy.html` ve `support.html` dosyalarını **aynı klasöre** koyun (destek sayfası gizlilik sayfasına göreli bağlantı veriyor). İsterseniz `support.html`'in bir kopyasını `index.html` yapın.
- [ ] Depo → Settings → Pages → *Deploy from a branch* → `main` / `(root)` → Save.
- [ ] Adresler: `https://<kullanıcı-adı>.github.io/teoriakoe-fi/privacy-policy.html` → `PRIVACY_POLICY_URL`, `.../support.html` → `SUPPORT_URL`.
- [ ] İki sayfayı telefonda açıp kontrol edin (açık ve koyu modda okunuyor olmalı).

## 3. Kod tarafı (1.7.0'da tamamlandı)

- [x] `pubspec.yaml` → `version: 1.7.0+10`; Ayarlar ekranındaki sürüm de 1.7.0 (bir test ikisini karşılaştırıyor).
- [x] Cihazda görünen ad mağaza adıyla uyumlu: Android `@string/app_name` ve iOS `CFBundleDisplayName` → **Teoriakoe FI**.
- [x] Uygulama ikonu: `design/app_icons/04-finland-junction.svg` → Android adaptif katmanlar (+ Android 13 temalı ikon), iOS ikon seti ve `store/icon-512.png` (`python3 -I tool/app_icon/generate.py`).
- [x] iOS yalnızca iPhone ve dikey: `TARGETED_DEVICE_FAMILY = 1`; iPad ekran görüntüsü gerekmez.
- [x] `ITSAppUsesNonExemptEncryption = NO`, gizlilik manifesti `ios/Runner/PrivacyInfo.xcprivacy`, en/fi/sv dil klasörleri (App Store üç dili listeler).
- [x] Ayarlar → Hakkında: "bağımsız uygulama, Traficom/Ajovarma ile bağlantısı yok" notu ve Autoliitto kaynak gösterimi uygulamanın içinde de var.
- [x] Uygulama içindeki "Real theory-test questions" etiketi "Autoliitto practice questions / Autoliiton harjoituskysymykset / Autoliittos övningsfrågor" oldu; mağaza metniyle çelişmiyor.
- [x] Hazır derlemeler `dist/` klasöründe: `TeoriakoeFI-1.7.0.aab` (Play'e yüklenecek dosya, **yükleme anahtarıyla imzalı**) ve APK'lar.

---

## 4. Google Play Console

### 4.1 Hesap
- [ ] Play Console geliştirici hesabı (tek seferlik 25 USD) ve kimlik doğrulaması tamam.
- [ ] **Kişisel hesap** 13 Kasım 2023'ten sonra açıldıysa: üretime (production) çıkmadan önce **kapalı testte en az 12 test kullanıcısı 14 gün boyunca** kayıtlı kalmalı. Zamanlamayı buna göre planlayın. (Kuruluş hesaplarında bu şart yok.)
- [ ] AB Dijital Hizmetler Yasası (DSA) "tacir / tacir değil" beyanı (Play Console → Ayarlar → Geliştirici hesabı).

### 4.2 Uygulamayı oluşturun
- [ ] *Tüm uygulamalar → Uygulama oluştur*
  - Uygulama adı: `Teoriakoe FI – Driving Test`
  - Varsayılan dil: **English (United States) – en-US**
  - Uygulama / Oyun: **Uygulama** · Ücretli / Ücretsiz: **Ücretsiz** (sonradan ücretliye çevrilemez)
  - Geliştirici Program Politikaları ve ABD ihracat yasaları beyanlarını onaylayın.

### 4.3 Yükleme anahtarı (upload key) ve Play App Signing
- [x] Yükleme anahtarı oluşturuldu: `../signing/teoriakoe-upload.jks` (alias `upload`, RSA 4096, 2054'e kadar geçerli). Şifreler ve parmak izleri: `../signing/teoriakoe-imza-bilgileri.txt`.
- [x] `android/key.properties` yazıldı; release derlemeleri artık otomatik olarak bu anahtarla imzalanıyor.
- [ ] `.jks` dosyasını ve bilgi dosyasını parola yöneticisine ve en az bir harici yedeğe alın. Kimseyle paylaşmayın.
- [ ] Play App Signing: ilk sürümde "Google tarafından oluşturulan uygulama imzalama anahtarı" seçeneğini kullanın; siz yalnızca yükleme anahtarıyla imzalarsınız.

### 4.4 AAB derleyin
- [ ] ```sh
  flutter build appbundle --release
  ```
  Çıktı: `build/app/outputs/bundle/release/app-release.aab`
- [ ] İmzayı kontrol edin (sertifika sahibi "Android Debug" **olmamalı**):
  ```sh
  keytool -printcert -jarfile build/app/outputs/bundle/release/app-release.aab
  ```

### 4.5 Önce dahili test (Internal testing)
- [ ] *Test → Dahili test → Test kullanıcıları*: e-posta listesi oluşturun, kendinizi ekleyin.
- [ ] *Yeni sürüm oluştur* → `app-release.aab` yükleyin → Sürüm adı `1.7.0 (10)`.
- [ ] Sürüm notları: `store/android/metadata/{en-US,fi-FI,sv-SE}/changelogs/10.txt`.
- [ ] *Sürümü incele → Dahili teste yayınla*. Katılım bağlantısını telefonda açıp Play'den yükleyin; **uçak modunda** da deneyin.
- [ ] Gerekirse kapalı test (12 kişi / 14 gün, bkz. 4.1), sonra üretim.

### 4.6 Ana mağaza girişi (Store listing)
- [ ] en-US: `title.txt` → Uygulama adı, `short_description.txt` → Kısa açıklama, `full_description.txt` → Tam açıklama.
- [ ] *Çeviri ekle* → **Finnish – fi-FI** ve **Swedish – sv-SE**: aynı alanları kendi klasörlerinden kopyalayın (makine çevirisi seçmeyin).
- [ ] Grafikler: uygulama simgesi 512×512 → `store/icon-512.png`; öne çıkan grafik 1024×500 ve telefon ekran görüntüleri → `store/android/metadata/<dil>/images/`.

### 4.7 Mağaza ayarları
- [ ] Uygulama kategorisi: **Education (Eğitim)**
- [ ] Etiketler (en fazla 5, listeden en yakın olanlar): ör. *Exam prep / Test preparation*, *Driving*, *Learning*, *Reference*, *Quiz*. Listede tam karşılığı olmayanları atlayın.
- [ ] İletişim bilgileri: e-posta `SUPPORT_EMAIL`, web sitesi `SUPPORT_URL`.

### 4.8 Uygulama içeriği (Policy → App content) – kesin cevaplar
- [ ] **Gizlilik politikası:** `PRIVACY_POLICY_URL`
- [ ] **Reklamlar:** Hayır, uygulamam reklam içermiyor.
- [ ] **Uygulama erişimi:** Tüm işlevler özel erişim gerektirmeden kullanılabilir (giriş yok).
- [ ] **İçerik derecelendirmesi (IARC anketi):**
  - E-posta: `SUPPORT_EMAIL` · Kategori: **Referans, Haber veya Eğitim** (*Reference, News, or Educational*)
  - Şiddet: Hayır · Cinsellik/çıplaklık: Hayır · Küfür/kaba dil: Hayır · Uyuşturucu, alkol, tütün kullanımı: Hayır · Kumar / simüle kumar: Hayır · Korku içeriği: Hayır
  - Kullanıcılar arası etkileşim / sohbet: Hayır · Kullanıcı içeriği paylaşımı: Hayır · Konum paylaşımı: Hayır · Dijital ürün satın alma: Hayır · Web tarayıcı / arama motoru: Hayır
  - Beklenen sonuç: **PEGI 3 / ESRB Everyone / USK 0**
  - Not: "Sürücünün kondisyonu" konusunda yasal alkol sınırı (0,5 promil) eğitim amacıyla geçiyor; alkol kullanımı gösterilmiyor veya özendirilmiyor. Anket açıkça "herhangi bir atıf" soruyorsa dürüst cevap verin; sonuç farklı çıkarsa bu not geçerli olur.
- [ ] **Hedef kitle ve içerik:** yaş grupları **16-17** ve **18 ve üzeri** (daha küçük yaşları seçmeyin → *Aileler / Families* programına girmez). "Uygulama çocuklara hitap ediyor mu?" sorulursa: **Hayır**.
- [ ] **Veri güvenliği (Data safety):** "Uygulamanız gerekli kullanıcı verisi türlerinden herhangi birini topluyor veya paylaşıyor mu?" → **Hayır**. Sonuç etiketi: *Veri toplanmıyor · Üçüncü taraflarla veri paylaşılmıyor*. (Yalnızca cihazda kalan veriler "toplama" sayılmaz.)
- [ ] **Reklam kimliği (Advertising ID):** Hayır, kullanılmıyor.
- [ ] **Haber uygulaması:** Hayır · **Devlet uygulaması:** Hayır · **Finansal özellikler:** Hiçbiri · **Sağlık özellikleri:** Hiçbiri.
- [ ] Hassas izin beyanı gerekmez (konum, fotoğraf/video, ön plan hizmeti vb. yok).

### 4.9 Fiyat ve ülkeler
- [ ] Ücretsiz · Ülkeler: **tüm ülkeler** (en azından Finlandiya, İsveç ve AB ülkeleri).

### 4.10 Üretime gönderin
- [ ] *Üretim → Yeni sürüm oluştur* → test edilen AAB'yi kitaplıktan seçin → notlar `changelogs/10.txt` → *İncelemeye gönder*. İlk inceleme birkaç gün sürebilir.

---

## 5. App Store Connect

### 5.1 Hesap
- [ ] Apple Developer Program üyeliği (yıllık 99 USD). Bireysel hesapta mağazadaki satıcı adı = `DEVELOPER_NAME`.
- [ ] *Business (İş)* → Ücretsiz uygulama sözleşmesi otomatik; ücretli sözleşme/vergi/banka gerekmez.
- [ ] *Business → Digital Services Act*: tacir durumu beyanı. Beyan olmadan uygulama **AB'de yayınlanmaz**.

### 5.2 Bundle ID ve uygulama kaydı
- [ ] developer.apple.com → *Identifiers* → App ID `fi.roadsigns.finnishRoadSigns` (özel yetenek gerekmez; Xcode otomatik de oluşturabilir).
- [ ] App Store Connect → *Uygulamalar → + → Yeni Uygulama*:
  - Platform: iOS · Ad: `Teoriakoe FI – Driving Test` · Birincil dil: **English (U.S.)**
  - Bundle ID: `fi.roadsigns.finnishRoadSigns` · SKU: `teoriakoe-fi-ios` · Kullanıcı erişimi: Tam erişim

### 5.3 Xcode imzalama, arşiv ve yükleme
- [ ] `open ios/Runner.xcworkspace` → *Runner* hedefi → *Signing & Capabilities*: **Team** seçin, *Automatically manage signing* açık, Bundle Identifier `fi.roadsigns.finnishRoadSigns`.
- [ ] *General → Supported Destinations*: yalnızca **iPhone** olduğunu doğrulayın (proje zaten böyle ayarlı).
- [ ] Derleyin:
  ```sh
  flutter build ipa --release
  ```
  Çıktıyı (`build/ios/ipa/*.ipa`) **Transporter** uygulamasıyla yükleyin. Alternatif: Xcode → *Product → Archive* → *Organizer → Distribute App → App Store Connect → Upload*.

### 5.4 TestFlight
- [ ] Derleme "Processing" bittikten sonra *TestFlight → Dahili Test* grubu oluşturun, kendinizi ekleyin, iPhone'da TestFlight ile kurun ve **uçak modunda** deneyin.
- [ ] Şifreleme (export compliance): `ITSAppUsesNonExemptEncryption = NO` ise sorulmaz. Sorulursa: muaf olmayan şifreleme **kullanılmıyor** ("None of the algorithms mentioned above").

### 5.5 Sürüm sayfası (1.7.0)
- [ ] Yerelleştirmeler: English (U.S.) + **Finnish** + **Swedish** ekleyin. Her dil için `store/ios/metadata/<dil>/`: Ad, Alt başlık, Tanıtım metni, Açıklama, Anahtar kelimeler, Destek URL'si, Pazarlama URL'si (boş olabilir).
- [ ] Ekran görüntüleri: 6,9" iPhone (ör. 1320×2868) → `store/ios/screenshots/`.
- [ ] *Bu sürümdeki yenilikler*: ilk sürümde bu alan görünmez; `release_notes.txt` sonraki sürümlerde kullanılır. fastlane ilk yüklemede "whatsNew" hatası verirse `release_notes.txt` dosyalarını geçici olarak klasör dışına taşıyın.
- [ ] Telif hakkı: `copyright.txt` · Birincil kategori: **Education** · İkincil kategori: **Reference**.
- [ ] *App Review Information*: "Sign-in required" **kapalı** (demo hesap yok); ad, soyad, telefon, e-posta `review_information/` dosyalarından; Notlar: `review_information/notes.txt`.
- [ ] *Version Release*: elle veya onaydan sonra otomatik.

### 5.6 App Privacy (Gizlilik etiketi)
- [ ] Gizlilik politikası URL'si: `PRIVACY_POLICY_URL`.
- [ ] "Bu uygulamadan veri topluyor musunuz?" → **Hayır** → etiket: **Data Not Collected** → *Publish*.
- [ ] İzleme (tracking) yok; App Tracking Transparency istemi kullanılmaz.

### 5.7 Yaş derecelendirmesi
- [ ] Tüm içerik soruları **None / Yok**: karikatür veya fantastik şiddet, gerçekçi şiddet, uzun süreli/sadistik şiddet, küfür veya kaba mizah, olgun/müstehcen temalar, korku, tıbbi/tedavi bilgisi, alkol-tütün-uyuşturucu kullanımı veya atfı, simüle kumar, cinsellik/çıplaklık, yarışmalar.
- [ ] Tüm evet/hayır soruları **No / Hayır**: sınırsız web erişimi, kullanıcı içeriği, mesajlaşma, reklam, kumar, ganimet kutusu (loot box), ebeveyn denetimi, yaş doğrulama, sağlık/zindelik konuları.
- [ ] Beklenen sonuç: **4+**.
- [ ] **Karar sizin:** içerikte yasal alkol sınırı ve temel ilk yardım bilgisi **eğitim/trafik güvenliği bağlamında** geçiyor. Çoğu sürücü kursu uygulaması bunları "None" olarak işaretler; daha temkinli olmak isterseniz ilgili sorularda "Infrequent" seçin – bu derecelendirmeyi yükseltebilir.

### 5.8 Diğer beyanlar
- [ ] **Content Rights:** "Uygulamanız üçüncü taraf içeriği içeriyor, gösteriyor veya erişiyor mu?" → **Evet**; "Gerekli tüm haklara sahip misiniz?" → **Evet** (Autoliitto ry'nin izni – izin yazısını saklayın, Apple isteyebilir).
- [ ] **Fiyatlandırma:** Ücretsiz (0).
- [ ] **Kullanılabilirlik:** tüm ülke ve bölgeler (en azından Finlandiya, İsveç ve AB). İsteğe bağlı: "Apple silikonlu Mac'lerde" kullanılabilirliği kapatın.
- [ ] *İncelemeye ekle → İncelemeye gönder*.

---

## 6. fastlane ile yükleme (isteğe bağlı)

İlk AAB'yi Play Console'a elle yükledikten ve bir hizmet hesabı JSON anahtarı (Play) / App Store Connect API anahtarı (Apple) oluşturduktan sonra:

```sh
# Google Play – yalnızca metinler (ekran görüntüleri hazırsa --skip_upload_images kaldırın)
fastlane supply --package_name fi.roadsigns.finnish_road_signs \
  --metadata_path store/android/metadata --json_key <play-hizmet-hesabi>.json \
  --skip_upload_apk --skip_upload_aab --skip_upload_images --skip_upload_screenshots

# App Store – metinler ve ekran görüntüleri, binary olmadan
fastlane deliver --app_identifier fi.roadsigns.finnishRoadSigns \
  --metadata_path store/ios/metadata --screenshots_path store/ios/screenshots \
  --api_key_path <asc-api-key>.json --skip_binary_upload \
  --precheck_include_in_app_purchases false
```

İpucu: Apple, Finlandiya/İsveç mağazalarında İngilizce için en-GB yerelleştirmesini de dizinleyebilir. İsterseniz `store/ios/metadata/en-US` klasörünü `en-GB` olarak kopyalayın.

---

## 7. Mağaza metinleri özeti

| Alan | en | fi | sv |
|---|---|---|---|
| Play başlık / App Store adı (≤30) | Teoriakoe FI – Driving Test | Teoriakoe FI – Ajokortti | Teoriakoe FI – Körkort |
| App Store alt başlık (≤30) | Finnish theory test & signs | Ajokortin teoriakoe ja merkit | Teoriprov och vägmärken |

Her açıklamada zorunlu iki cümle bulunur ve test bunları kontrol eder:
- Feragat: Traficom, Ajovarma ve Finlandiya devleti ile bağlantı veya onay yoktur.
- Atıf: "Practice questions and photographs: Autoliiton Ajokoulu (Autoliitto ry), used with permission." (ve fi/sv karşılıkları)

"Official app" ifadesini hiçbir metinde kullanmayın; test bunu da kontrol eder.
