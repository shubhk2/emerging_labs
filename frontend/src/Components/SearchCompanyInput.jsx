// src/components/SearchCompanyInput.js
import { useEffect, useRef, useState } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { Link } from 'react-router-dom';
import { loading, resetSearchResults, searchCompanyFunc } from '../Redux/MainReducer/action';
import { VscLoading } from "react-icons/vsc";
import "../Styles/Components/SearchCompanyInput.css";

export const SearchCompanyInput = () => {
    const dispatch = useDispatch();
    const { selectedCompany, searchedCompanies, isLoading } = useSelector(state => state.mainReducer);

    const [search, setSearch] = useState(selectedCompany.companyName || '');
    const [searchFocused, setSearchFocused] = useState(false);
    const debounceTimeout = useRef(null);

    const handleCompanySearch = (event) => {
        dispatch(loading());
        const searchString = event?.target?.value?.trim() || '';
        setSearch(event?.target?.value || '');

        clearTimeout(debounceTimeout.current);
        debounceTimeout.current = setTimeout(() => {
            if (searchString.length >= 1) {
                dispatch(searchCompanyFunc(searchString));
            } else {
                dispatch(resetSearchResults());
            }
        }, 500);
    };

    useEffect(() => { dispatch(resetSearchResults()) }, [dispatch])
    useEffect(() => { setSearch(selectedCompany.companyName || '') }, [selectedCompany]);

    return (
        <div className="search-company-input">
            <input type='text' value={search} onChange={handleCompanySearch} placeholder='Search For Company' onFocus={() => setSearchFocused(true)} onBlur={() => setTimeout(() => setSearchFocused(false), 350)} />
            {
                searchFocused && <div className="searched-companies-container">
                    {
                        (isLoading && <div className="loading"><VscLoading /></div>) ||
                        (searchedCompanies.length > 0 &&
                            <ul className="searched-companies">
                                {searchedCompanies.map((company) => (
                                    <li key={company.id}>
                                        <Link to={`/company/${company.id}`}>
                                            {company.full_name}
                                        </Link>
                                    </li>
                                ))}
                            </ul>
                        ) ||
                        (searchedCompanies.length === 0 && !search &&
                            <div className="no-results">Type to Search</div>
                        ) ||
                        (searchedCompanies.length === 0 && search &&
                            <div className="no-results">No results found</div>
                        )
                    }
                </div>
            }
        </div>
    );
};
