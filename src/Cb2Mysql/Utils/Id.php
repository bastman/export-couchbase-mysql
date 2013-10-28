<?php
/**
 * Created by JetBrains PhpStorm.
 * User: seb
 * Date: 12/21/12
 * Time: 4:01 PM
 * To change this template use File | Settings | File Templates.
 */
namespace Cb2Mysql\Utils;

/**
 * Class Id
 * @package Cb2Mysql\Utils
 */
class Id
{

    /**
     * A VALID ID IS: UNSIGNED_INT>0 (or ctype_digit_string castable to int>0)
     * @param int|string $id
     * @return bool
     */
    public static function isValidId($id)
    {
        $result = false;

        if ((is_int($id)) && ($id > 0)) {

            return true;
        }
        if ((is_string($id)) && (ctype_digit($id))) {

            $id = (int)$id;

            return ($id > 0);
        }

        return $result;
    }

    /**
     * returns int if valid id
     * returns defaultValue if not a valid id
     * @param int|string|mixed $id
     * @param mixed $defaultValue
     * @return int|mixed
     */
    public static function castId($id, $defaultValue)
    {
        $result = $defaultValue;

        if(!self::isValidId($id)) {

            return $result;
        }

        return (int)$id;
    }

    /**
     * @param int|string $value
     * @param mixed $defaultValue
     * @return int|mixed
     */
    public static function castUnsignedInt($value, $defaultValue)
    {
        $result = $defaultValue;

        if ((is_string($value)) && (ctype_digit($value))) {

            $value = (int)$value;
        }

        if ((is_int($value)) && ($value >= 0)) {

            return $value;
        }

        return $result;
    }

}
