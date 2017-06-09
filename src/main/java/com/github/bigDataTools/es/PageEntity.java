package com.github.bigDataTools.es;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * 数据分页bean
 * @author  winstone
 */
public class PageEntity<E> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 总记录数
	 */
	private Long totalCount = 0L;

	/**
	 * 当前第几页
	 */
	private int currentPageNo = 1;

	/**
	 * 总页数
	 */
	@SuppressWarnings("unused")
	private Long totalPageCount = 0L;

	/**
	 * 每页的记录数
	 */
	private int pageSize = 10;

	private List<E> contents = Lists.newArrayList();

	private Object backforward = null;

	public Object getBackforward() {
		return backforward;
	}

	public void setBackforward(Object backforward) {
		this.backforward = backforward;
	}

	public Long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Long totalCount) {
		this.totalCount = totalCount;
	}

	public int getCurrentPageNo() {
		return currentPageNo;
	}

	public void setCurrentPageNo(int currentPageNo) {
		this.currentPageNo = currentPageNo;
	}

	public Long getTotalPageCount() {
		long quotient = totalCount / pageSize;
		if ((quotient * pageSize) < totalCount) {
			quotient++;
		}
		setTotalPageCount(quotient);
		return quotient;
	}

	public void setTotalPageCount(Long totalPageCount) {
		this.totalPageCount = totalPageCount;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public List<E> getContents() {
		return contents;
	}

	public void setContents(List<E> contents) {
		this.contents = contents;
	}


	public static int getStartOfPage(int pageNo, int pageSize) {
		return (pageNo - 1) * pageSize;
	}
}
