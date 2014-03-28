<?php
header('Content-Type: text/html; charset=utf8');
mb_internal_encoding("UTF-8"); // Установка внутренней кодировки скрипта UTF-8
setlocale(LC_ALL, "ru_RU.UTF-8");
if(isset($_GET['DEBUG']) || true) define('DEBUG', true);
else  define('DEBUG', false);

// Конфиг
$allow_col_num	=	9; // Количество колонок во входном файле
$path	=	dirname(__file__).'/';
$file_input = '/home/lgashop/sync.lgashop.ru/docs/lga_base.csv';
$file_buffer = 'buffer.csv';
$file_errorlog = 'errors.csv';
$file_refresh['spb'] = 'refresh.spb.dat';
$tablename['spb'] = 'import_temp_spb';
require_once $path.'pdo.config.php';		// Подключение конфига БД
extract($dbconfig);
$prefix = 'modx_'; // Префикс таблиц

// Инициализация
define('MODX_API_MODE', true); // API MODX
require_once $_SERVER['DOCUMENT_ROOT'].'/index.php'; // MODX
$modx->getService('error','error.modError');
$modx->setLogLevel(modX::LOG_LEVEL_INFO);
$modx->setLogTarget(XPDO_CLI_MODE ? 'ECHO' : 'HTML');
$arr=file($file_input);
$count=0;

// Функции
function logWrite($str, $fh=null)
{
	if(DEBUG) print "log: $str\n";
	// TODO:  Запись в errorlog
}


if(DEBUG) print "<pre>\n";
// ОТКРЫТИЕ buffer.csv
// -------------------------------------------------------\
$fhBuf = fopen($path.$file_buffer, "w");
ftruncate($fhBuf, 0); // очищаем файл до 0 байтов.
$locked = flock($fhBuf, LOCK_EX | LOCK_NB);
if(!$locked) {
    echo 'Не удалось получить блокировку';
    exit(-1);
}

logWrite(date('j-m-Y H:i:s'));
logWrite('Чтение файла lga_base.csv');

foreach($arr as $str)
{
	$fields=explode(';',$str);
	$num=count($fields);
	if($fields[1]=='Артикул') {logWrite("Строка заголовков удалена"); continue;}
	if($num!=$allow_col_num) {logWrite($str); continue;}
	$count++;
	fwrite($fhBuf, "$str");
}
$output = "Запись в буфер: ".$count." строк к импорту";
logWrite($output);

// ЗАКРЫТИЕ buffer.csv
fflush($fhBuf) or die($php_errormsg);
flock($fhBuf,LOCK_UN) or die($php_errormsg);
fclose($fhBuf) or die($php_errormsg);
unset($fhBuf);
/**/
// ---------------------------------------------------/

// Подключаемся к БД
try {
	$db = new PDO("mysql:host=$dbhost;dbname=$dbname", $dbuser, $dbpass, array(
		PDO::ATTR_PERSISTENT => true,
		PDO::MYSQL_ATTR_LOCAL_INFILE => true
	));
} catch (PDOException $e) {
    logWrite('Подключение к БД не удалось: '.$e->getMessage());
	exit(13);
}

$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$db->exec("set names utf8");

// ------------ Импорт buffer.csv -> БД ------------------------
$db->exec("TRUNCATE TABLE `import_temp_raw`;");
$res=$db->exec("LOAD DATA LOCAL
INFILE '".$path."buffer.csv' INTO TABLE `import_temp_raw`
fields terminated by ';'
enclosed by ''
lines terminated by '\n';");
logWrite('Результат импорта в БД: '.$res.' строк внесено');
/**/

//------------- САНКТ-ПЕТЕРБУРГ -----------------------------------------------------------------------\
// Генерируем временную таблицу по Питеру
$sql="CREATE TEMPORARY TABLE IF NOT EXISTS temp_import_spb AS (
SELECT `id`,`article`,`name`,`vendor`,`weight`,
SUBSTRING_INDEX( `supply` , '=', -1 ) AS `quantity`,
SUBSTRING_INDEX( `price` , '=', -1 ) AS `price`,
SUBSTRING_INDEX( `old_price` , '=', -1 ) AS `old_price`
FROM `import_temp_raw`
ORDER BY `article` ASC
)";
$res=$db->exec($sql);
logWrite('Сборка временной таблицы по СПб: '.$res.' строк внесено');

// Составляем список ресурсов с изменениями
$sql="SELECT m.id
FROM `modx_ms2_products` as m, temp_import_spb as t
WHERE m.article=t.article
AND (m.quantity<>t.quantity
OR m.weight<>t.weight
OR m.price<>t.price
OR m.old_price<>t.old_price)
ORDER BY `id` ASC";
$stmt=$db->query($sql);
$stmt->setFetchMode(PDO::FETCH_ASSOC);
$refreshIDs=array();
while($res = $stmt->fetch())
{
	$refreshIDs[$res['id']]='';
}
//print_r($refreshIDs);
logWrite("Изменения обнаружены в: ".count($refreshIDs)." товарах");

// TODO: Сохраняем сериализованный массив ресурсов с изменениями в файл refresh.spb.dat
// А может и не надо...

// Заливаем новые данные в рабочую таблицу ms_products
$sql="UPDATE `modx_ms2_products` as m, temp_import_spb as t, `modx_site_content` as c
SET m.quantity=t.quantity,
m.weight=t.weight,
m.price=t.price,
m.old_price=t.old_price
WHERE m.article=t.article
AND m.id=c.id
AND c.context_key='web'";
$res=$db->exec($sql);
logWrite('Запись новых данных: '.$res.' строк внесено');
/**/
//------------- САНКТ-ПЕТЕРБУРГ  ВЫПОЛНЕНО ---------------------------------------------------------------------/

// Избирательная чистка Кэша
//print_r($refreshIDs);
foreach($refreshIDs as $id => $val)
{
	//$id=79;
	$resource=$modx->getObject('modResource',$id);
	$cacheKey = $resource->getCacheKey();
	$modx->cacheManager->refresh(array(
		'resource' => array('key' => $cacheKey),
	));
}

// Финиш
logWrite('затраченное время: '.(microtime(true) - $modx->startTime));
logWrite('');
if(DEBUG) print "</pre>";
