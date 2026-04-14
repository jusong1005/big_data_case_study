package com.tipdm.repository;


import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.io.Serializable;

/**
 * Created by ch on 2018/10/31
 */
@NoRepositoryBean
public interface BaseReporitory<T, ID extends Serializable> extends
        PagingAndSortingRepository<T, ID>, JpaSpecificationExecutor<T> {

    public Page<T> findAll(Pageable pageable);
}
