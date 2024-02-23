package com.zy.bedrock.slate.utils;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/22;
 * @description 断言工具类
 */

@SuppressWarnings("unused")
public final class Assert {
	/**
	 * 非空对象检查
	 * @param property 受检对象
	 * @return true 则代表受检对象不为 null
	 */
	public static boolean isNotNull(Object property) {
		return property != null;
	}

	/**
	 * 非空检查
	 * @param obj 受检对象
	 * @return 若不为 null 且不为空集则返回 true
	 */
	public static boolean isNotNull(Object[] obj) {
		return obj != null && obj.length != 0;
	}

	/**
	 * 非空或空集检查
	 * @param obj 集合类型参数
	 * @return 若不为 null 且不为空集则返回 true
	 */
	public static boolean isNotNull(Collection<?> obj) {
		return obj != null && !obj.isEmpty();
	}

	/**
	 * 非空检查
	 * @param obj 受检对象
	 * @param message 条件不成立时返回的错误信息
	 * @throws IllegalArgumentException - 为 null 则抛出
	 */
	public static void notNull(Object obj, String message) {
		if(obj == null) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * 非空与空集检查
	 * @param obj 受检对象
	 * @param message 条件不成立时返回的错误信息
	 * @throws IllegalArgumentException - 为 null 或空集则抛出
	 */
	public static void notNull(Object[] obj, String message) {
		if(!isNotNull(obj)) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * 非空或空集检查
	 * @param obj 受检对象
	 * @param message 条件不成立时返回的错误信息
	 */
	public static void notNull(Collection<?> obj, String message) {
		if(!isNotNull(obj)) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * 空串检查
	 * @param checkStr 受检对象
	 * @throws IllegalArgumentException 为 null 或空串则抛出
	 */
	public static void hasText(String checkStr, String message) {
		if (isEmptyText(checkStr)) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * 空串检查
	 * @param checkStr 受检字符串
	 * @param messageSupplier 条件不成立时返回错误信息的 Supplier 实例
	 * @throws IllegalArgumentException 对象集存在空串则抛出
	 */
	public static void hasText(String checkStr, Supplier<String> messageSupplier) {
		hasText(checkStr, messageSupplier.get());
	}


	/**
	 * 空串检查
	 * @param checkStr 受检字符串
	 * @return 若为 null 或空串则返回 true
	 */
	public static boolean isEmptyText(String checkStr) {
		return checkStr == null || checkStr.isEmpty();
	}

	/**
	 * 条件检查
	 * @param flag 检查条件
	 * @param message 条件不成立时返回的错误信息
	 * @throws IllegalArgumentException 条件不成立则抛出
	 */
	public static void isTrue(boolean flag, String message) {
		if (!flag) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * 条件判断
	 * @param flag 判断条件
	 * @param messageSupplier 条件不成立时返回错误信息的 Supplier 实例
	 * @throws IllegalArgumentException 条件不成立则抛出
	 */
	public static void isTrue(boolean flag, Supplier<String> messageSupplier){
		isTrue(flag, messageSupplier.get());
	}

	/**
	 * 判断指定对象是否是指定类型的实例，不是则抛出异常
	 * @param requiredType 指定类型
	 * @param obj 指定对象
	 * @param message 条件不成立时返回的错误信息
	 * @throws IllegalArgumentException 指定对象类不是指定类型的实现或子类则抛出
	 */
	public static void isInstanceOf(Class<?> requiredType, Object obj, String message) {
		isTrue(requiredType.isInstance(obj),message );
	}

	/**
	 * 判断指定对象是否是指定类型的实例，不是则抛出异常
	 * @param requiredType 指定类型
	 * @param obj 指定对象
	 * @throws IllegalArgumentException 指定对象类不是指定类型的实现或子类则抛出
	 */
	public static void isInstanceOf(Class<?> requiredType, Object obj) {
		notNull(requiredType , "'requiredType' 不能为 null");
		notNull(obj , "'obj' 不能为 null");
		isTrue(requiredType.isInstance(obj), () -> obj.getClass().getName() +
				"类不是 " +
				requiredType.getName() +
				"的实现或子类");
	}

	/**
	 * 判断 superType 是否是 subType 的父类或它本身
	 * @param superType 要根据 subType 检查的超类型
	 * @param subType  要检查的子类型
	 * @param message 条件不成立时返回的错误信息
	 */
	public static void isAssignable(Class<?> superType, Class<?> subType, String message) {
		isTrue(superType.isAssignableFrom(subType), message);
	}

	/**
	 * 判断 superType 是否是 subType 的父类或它本身
	 * @param superType 要根据 subType 检查的超类型
	 * @param subType  要检查的子类型
	 */
	public static void isAssignable(Class<?> superType, Class<?> subType) {
		isTrue(superType.isAssignableFrom(subType), () -> superType.getName() +
				"不是 " +
				subType.getName() +
				"的父类或它本身，无法进行类型转换"
		);
	}
}
