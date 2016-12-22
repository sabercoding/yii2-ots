<?php
namespace SaberCoding\Ots;

use \Aliyun\OTS\OTSClient as OTSClient;
use yii\base\Component;
use yii\db\Exception;

/**
 * Class Client
 * @package SaberCoding\Ots
 */
class Client extends Component {
    protected static $ots_client;
    public $EndPoint = '';
    public $AccessKeyID = '';
    public $AccessKeySecret = '';
    public $InstanceName = '';
    public $DebugLogHandler = null;
    public $ConnectionTimeout = 5.0;
    public $SocketTimeout = 5.0;
    public $ErrorLogHandler = null;
    protected function ots() {
        if (!isset(self::$ots_client)) {
            $config = array(
                'EndPoint' => $this->EndPoint,
                'AccessKeyID' => $this->AccessKeyID,
                'AccessKeySecret' => $this->AccessKeySecret,
                'InstanceName' => $this->InstanceName,
                'DebugLogHandler' => $this->DebugLogHandler,
                'ConnectionTimeout' => $this->ConnectionTimeout,
                'SocketTimeout' => $this->SocketTimeout,
                'ErrorLogHandler' => $this->ErrorLogHandler,
            );
            if (!$config) {
                throw new \Exception(get_class($this) . ' need be configured. ');
            }
            self::$ots_client= new OTSClient($config);
        }
        return self::$ots_client;
    }

    /**
     * 写入一行数据。
     * @param $table
     * @param array $primary
     * @param array $columns
     * @param string $condition
     * @return bool
     * @throws \Exception
     */
    public function putRow($table, array $primary, array $columns, $condition = 'IGNORE') {
        $request = array(
            'table_name' => $table,
            'condition' => $condition,
            'primary_key' => $primary,
            'attribute_columns' => $columns,
        );
        $response = $this->executeCommand('putRow', array($request));
        return (bool)$response['consumed']['capacity_unit']['write'];
    }

    /**
     * 写入多行数据
     * @param $table
     * @param $batch_datas
     * @param string $condition
     * @return mixed
     * @throws \Exception
     */
    public function batchPutRow($table, $batch_datas, $condition = 'IGNORE') {
        $put_rows = array();
        foreach ($batch_datas as $k) {
            $put_rows[] = array(
                'condition' => $condition,
                'primary_key' => $k['primary'],
                'attribute_columns' => $k['columns'],
            );
        }
        $request = array(
            'tables' => array(
                array(
                    'table_name' => $table,
                    'put_rows' => $put_rows,
                ),
            ),
        );
        $response = $this->executeCommand('batchWriteRow', array($request));
        return $response['tables'][0]['put_rows'];
    }

    /**
     * 更新多行数据
     * @param $table
     * @param array $batch_datas
     * @param string $condition
     * @return mixed
     * @throws \Exception
     */
    public function batchUpdateRow($table, array $batch_datas, $condition = 'IGNORE') {
        $update_rows = array();
        foreach ($batch_datas as $k) {
            $update_rows[] = array(
                'condition' => $condition,
                'primary_key' => $k['primary'],
                'attribute_columns_to_put' => $k['columns'],
            );
        }
        $request = array(
            'tables' => array(
                array(
                    'table_name' => $table,
                    'update_rows' => $update_rows,
                ),
            ),
        );
        $response = $this->executeCommand('batchWriteRow', array($request));
        return $response['tables'][0]['update_rows'];
    }

    /**
     * 删除多行数据
     * @param $table
     * @param $batch_datas
     * @param string $condition
     * @return mixed
     * @throws \Exception
     */
    public function batchDeleteRow($table, $batch_datas, $condition = 'IGNORE') {
        $delete_rows = array();
        foreach ($batch_datas as $k) {
            $delete_rows[] = array(
                'condition' => $condition,
                'primary_key' => $k['primary'],
            );
        }
        $request = array(
            'tables' => array(
                array(
                    'table_name' => $table,
                    'delete_rows' => $delete_rows,
                ),
            ),
        );
        $response = $this->executeCommand('batchWriteRow', array($request));
        return $response['tables'][0]['delete_rows'];
    }


    /**
     * 读取一行数据
     * @param $table
     * @param array $primary
     * @param array $columns
     * @return mixed
     * @throws \Exception
     */
    public function getRow($table, array $primary, array $columns = array()) {
        $request = array(
            'table_name' => $table,
            'primary_key' => $primary,
            'columns_to_get' => $columns
        );
        $response = $this->executeCommand('getRow', array($request));
        return $response['row']['attribute_columns'];
    }


    /**
     * 读取指定的多行数据。
     * @param $table
     * @param array $primarys
     * @return array
     * @throws \Exception
     */
    public function batchGetRow($table, array $primarys) {
        $rows = array();
        foreach ($primarys as $parimary) {
            $rows[] = array('primary_key' => $parimary);
        }
        $request = array(
            'tables' => array(
                array(
                    'table_name' => $table,
                    'rows' => $rows,
                ),
            ),
        );
        $response = $this->executeCommand('batchGetRow', array($request));
        $batch_result = array();
        if (count($response['tables'][0]['rows']) > 0) {
            foreach ($response['tables'][0]['rows'] as $list) {
                if ($list['is_ok'] && $list['row']['primary_key_columns']) {
                    $batch_result[] = $list['row'];
                }
                unset($list);
            }
        }
        return $batch_result;
    }


    /**
     * 读取一个范围的数据
     * @param $table
     * @param $startPK
     * @param $endPK
     * @param string $direction
     * @param array $columns
     * @param int $limit
     * @return mixed
     * @throws \Exception
     */
    public function getRange($table, $startPK, $endPK, $direction = 'FORWARD', array $columns = array(), $limit = 10) {
        if ($direction != 'FORWARD') {
            $direction = 'BACKWARD';
        }

        $request = array(
            'table_name' => $table,
            'direction' => $direction,                         // 方向可以为 FORWARD 或者 BACKWARD
            'inclusive_start_primary_key' => $startPK,         // 开始主键
            'exclusive_end_primary_key' => $endPK,             // 结束主键
            'columns_to_get' => $columns,
            'limit' => $limit
        );
        $response = $this->executeCommand('getRange', array($request));

        unset($response['consumed']);
        return $response;
    }


    /**
     * 更新一行数据
     * @param $table
     * @param array $primary
     * @param array $columns
     * @param bool $put
     * @param string $condition
     * @return bool
     * @throws \Exception
     */
    public function updateRow($table, array $primary, array $columns, $put = true, $condition = 'IGNORE') {
        if ($put) {
            $request = array(
                'table_name' => $table,
                'condition' => $condition,
                'primary_key' => $primary,
                'attribute_columns_to_put' => $columns
            );
        } else {
            $request = array(
                'table_name' => $table,
                'condition' => $condition,
                'primary_key' => $primary,
                'attribute_columns_to_delete' => $columns
            );
        }

        $response = $this->executeCommand('updateRow', array($request));
        return (bool)$response['consumed']['capacity_unit']['write'];
    }


    /**
     * 删除一行数据
     * @param $table
     * @param array $primary
     * @param string $condition
     * @return bool
     * @throws \Exception
     */
    public function deleteRow($table, array $primary, $condition = 'IGNORE') {
        $request = array(
            'table_name' => $table,
            'condition' => $condition,
            'primary_key' => $primary,
        );
        $response = $this->executeCommand('deleteRow', array($request));
        return (bool)$response['consumed']['capacity_unit']['write'];
    }

    /**
     * 将调用均转向到封装的OTS实例
     * @param $name
     * @param array $args
     * @return mixed
     * @throws \Exception
     */
    public function __call($name, $args = array()) {
        return $this->executeCommand($name, $args);
    }

    /**
     * 捕获异常.抓取strace
     * @param $name
     * @param array $args
     * @return mixed
     * @throws \Exception
     */
    private function executeCommand($name, array $args = array()) {
        try {
            $ret = call_user_func_array(array($this->ots(), $name), $args);
        } catch (\Exception $e) {
            if ($e instanceof \Exception) {
                throw $e;
            } else {
                throw new Exception($e->getCode() . ' ' . $e->getMessage());
            }
        }
        return $ret;
    }
}